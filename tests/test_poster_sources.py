import tempfile
import unittest
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from PIL import Image

from mv_scraper.cli import (
    ParsedTrack,
    build_plot_text,
    clean_youtube_description_for_plot,
    DEFAULT_OPENAI_MODEL,
    build_metadata,
    download_poster,
    find_fallback_poster_url,
    search_bugs_candidates,
    search_lgych_candidates,
    search_youtube_candidates,
    write_nfo,
)


class PosterSourceTests(unittest.TestCase):
    def test_download_poster_uses_lgych_headers(self) -> None:
        image = Image.new("RGB", (2, 2), color=(0, 255, 0))
        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        jpeg_bytes = buffer.getvalue()

        ok_response = Mock()
        ok_response.content = jpeg_bytes
        ok_response.raise_for_status = Mock()

        with tempfile.TemporaryDirectory(prefix="mv_poster_headers_") as tmp:
            output = Path(tmp) / "poster.jpg"
            with patch("mv_scraper.cli.requests.get", return_value=ok_response) as mocked_get:
                written = download_poster(
                    "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg",
                    output,
                    timeout=8,
                    proxy_url=None,
                )

            self.assertTrue(written)
            headers = mocked_get.call_args.kwargs["headers"]
            self.assertIn("Mozilla", headers["User-Agent"])
            self.assertEqual(headers.get("Referer"), "https://www.lgych.com/")
            self.assertEqual(headers.get("Origin"), "https://www.lgych.com")

    def test_download_poster_retries_without_proxy_when_proxy_fails(self) -> None:
        image = Image.new("RGB", (2, 2), color=(255, 0, 0))
        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        jpeg_bytes = buffer.getvalue()

        proxy_error = requests.exceptions.ProxyError("Cannot connect to proxy")
        ok_response = Mock()
        ok_response.content = jpeg_bytes
        ok_response.raise_for_status = Mock()

        with tempfile.TemporaryDirectory(prefix="mv_poster_retry_") as tmp:
            output = Path(tmp) / "poster.jpg"
            with patch("mv_scraper.cli.requests.get", side_effect=[proxy_error, ok_response]) as mocked_get:
                written = download_poster(
                    "https://www.lgych.com/wp-content/uploads/2025/04/hel.jpg",
                    output,
                    timeout=8,
                    proxy_url="http://127.0.0.1:7890",
                )

            self.assertTrue(written)
            self.assertTrue(output.exists())
            self.assertEqual(mocked_get.call_count, 2)
            first_kwargs = mocked_get.call_args_list[0].kwargs
            second_kwargs = mocked_get.call_args_list[1].kwargs
            self.assertIn("proxies", first_kwargs)
            self.assertNotIn("proxies", second_kwargs)

    def test_download_poster_fallbacks_to_timthumb_after_403(self) -> None:
        image = Image.new("RGB", (2, 2), color=(0, 0, 255))
        buffer = BytesIO()
        image.save(buffer, format="JPEG")
        jpeg_bytes = buffer.getvalue()

        bad_response = Mock()
        bad_response.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Client Error: Forbidden")
        bad_response.content = b""

        ok_response = Mock()
        ok_response.raise_for_status = Mock()
        ok_response.content = jpeg_bytes

        with tempfile.TemporaryDirectory(prefix="mv_poster_timthumb_") as tmp:
            output = Path(tmp) / "poster.jpg"
            with patch("mv_scraper.cli.requests.get", side_effect=[bad_response, ok_response]) as mocked_get:
                written = download_poster(
                    "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg",
                    output,
                    timeout=8,
                    proxy_url=None,
                )

            self.assertTrue(written)
            self.assertEqual(mocked_get.call_count, 2)
            first_url = mocked_get.call_args_list[0].args[0]
            second_url = mocked_get.call_args_list[1].args[0]
            self.assertEqual(first_url, "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg")
            self.assertIn("timthumb.php?src=", second_url)

    def test_search_lgych_extracts_original_image_from_timthumb(self) -> None:
        html = """
        <html>
          <body>
            <img class="thumb" src="https://www.lgych.com/wp-content/uploads/2023/12/thumb-ing.gif"
                 data-src="https://www.lgych.com/wp-content/themes/modown/timthumb.php?src=https%3A%2F%2Fwww.lgych.com%2Fwp-content%2Fuploads%2F2025%2F04%2Fhel.jpg&w=285&h=285&zc=1"
                 alt="Adele - Hello" />
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response) as mocked_get:
            candidates = search_lgych_candidates(
                artist="Adele",
                title="Hello",
                timeout=8,
                proxy_url="http://127.0.0.1:7890",
                limit=5,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["artwork_url"], "https://www.lgych.com/wp-content/uploads/2025/04/hel.jpg")
        self.assertEqual(candidates[0]["collection_name"], "lgych.com")
        self.assertEqual(mocked_get.call_args.kwargs["params"]["s"], "Adele Hello")
        self.assertIn("Mozilla", mocked_get.call_args.kwargs["headers"]["User-Agent"])

    def test_search_lgych_keeps_apostrophe_in_title(self) -> None:
        html = """
        <html>
          <body>
            <img class="thumb"
                 data-src="https://www.lgych.com/wp-content/themes/modown/timthumb.php?src=https%3A%2F%2Fwww.lgych.com%2Fwp-content%2Fuploads%2F2023%2F10%2Fgir.jpg&w=285&h=285&zc=1"
                 alt="少女时代 Girl's Generation - FOREVER 1 4K 2160P [Bugs MP4 1.2GB]" />
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response):
            candidates = search_lgych_candidates(
                artist="少女时代",
                title="FOREVER 1",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["track_name"], "少女时代 Girl's Generation - FOREVER 1 4K 2160P [Bugs MP4 1.2GB]")

    def test_search_bugs_candidates_extracts_mv_tracks(self) -> None:
        html = """
        <html>
          <body>
            <table class="list trackList">
              <tbody>
                <tr albumId="4124472" artistId="1168" mvId="633889" trackId="6330908" rowType="track">
                  <td>
                    <img src="https://image.bugsm.co.kr/album/images/50/41244/4124472.jpg?version=1" alt="cover" />
                  </td>
                  <th scope="row">
                    <p class="title"><a title="Crazier">Crazier</a></p>
                  </th>
                  <td class="left">
                    <p class="artist"><a title="보아 (BoA)">보아 (BoA)</a></p>
                  </td>
                  <td class="left">
                    <a class="album" title="Crazier - The 11th Album">Crazier - The 11th Album</a>
                  </td>
                </tr>
                <tr albumId="0" artistId="0" mvId="0" trackId="999" rowType="track">
                  <td><img src="https://image.bugsm.co.kr/album/images/50/999.jpg" alt="skip" /></td>
                  <th scope="row"><p class="title"><a title="Skip Me">Skip Me</a></p></th>
                  <td class="left"><p class="artist"><a title="Other">Other</a></p></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response) as mocked_get:
            candidates = search_bugs_candidates(
                artist="BoA",
                title="Crazier",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["track_name"], "Crazier")
        self.assertEqual(candidates[0]["artist_name"], "보아 (BoA)")
        self.assertEqual(candidates[0]["mv_id"], "633889")
        self.assertEqual(candidates[0]["webpage_url"], "https://music.bugs.co.kr/track/6330908")
        self.assertIn("/album/images/1000/", candidates[0]["artwork_url"])
        self.assertEqual(mocked_get.call_args.kwargs["params"]["q"], "BoA Crazier")

    def test_search_youtube_candidates_skips_unavailable_entries(self) -> None:
        ydl_instance = Mock()
        ydl_instance.__enter__ = Mock(return_value=ydl_instance)
        ydl_instance.__exit__ = Mock(return_value=False)
        ydl_instance.extract_info.return_value = {
            "entries": [
                None,
                {
                    "id": "video-1",
                    "title": "ILLIT (아일릿) 'Tick-Tack' Official MV",
                    "channel": "HYBE LABELS",
                    "thumbnail": "https://i.ytimg.com/vi/video-1/maxresdefault.jpg",
                    "webpage_url": "https://www.youtube.com/watch?v=video-1",
                },
                {
                    "id": "video-2",
                    "title": "ILLIT (아일릿) 'Tick-Tack' Official MV (Performance ver.)",
                    "channel": "HYBE LABELS",
                    "thumbnail": "https://i.ytimg.com/vi/video-2/maxresdefault.jpg",
                    "webpage_url": "https://www.youtube.com/watch?v=video-2",
                },
            ]
        }

        with patch("mv_scraper.cli.YoutubeDL", return_value=ydl_instance) as mocked_ydl:
            candidates = search_youtube_candidates("ILLIT Tick-Tack official music video", max_results=2)

        options = mocked_ydl.call_args.args[0]
        self.assertTrue(options["ignoreerrors"])
        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0]["title"], "ILLIT (아일릿) 'Tick-Tack' Official MV")
        self.assertEqual(candidates[1]["title"], "ILLIT (아일릿) 'Tick-Tack' Official MV (Performance ver.)")

    def test_search_lgych_uses_anchor_title_when_alt_missing(self) -> None:
        html = """
        <html>
          <body>
            <a href="https://www.lgych.com/68675.html" title="少女时代 Girl's Generation - FOREVER 1 4K 2160P [Bugs MP4 1.2GB]">
              <img class="thumb"
                   src="https://www.lgych.com/wp-content/uploads/2023/12/thumb-ing.gif"
                   data-src="https://www.lgych.com/wp-content/themes/modown/timthumb.php?src=https%3A%2F%2Fwww.lgych.com%2Fwp-content%2Fuploads%2F2023%2F10%2Fgir.jpg&w=285&h=285&zc=1" />
            </a>
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response):
            candidates = search_lgych_candidates(
                artist="少女时代",
                title="FOREVER 1",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(len(candidates), 1)
        self.assertIn("FOREVER 1", candidates[0]["track_name"])
        self.assertEqual(candidates[0]["artwork_url"], "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg")

    def test_search_lgych_ignores_non_thumb_images(self) -> None:
        html = """
        <html>
          <body>
            <img src="https://www.lgych.com/wp-content/uploads/2023/07/weixin.jpg" alt="FOREVER 1" />
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response):
            candidates = search_lgych_candidates(
                artist="少女时代",
                title="FOREVER 1",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(candidates, [])

    def test_search_lgych_ignores_broken_html_title_payload(self) -> None:
        html = """
        <html>
          <body>
            <a href="#" title="蓝光演唱会><img src=&quot;https://www.lgych.com/wp-content/uploads/2020/11/weixin.jpg&quot; class=&quot;thumb&quot;">
              <img class="thumb"
                   data-src="https://www.lgych.com/wp-content/themes/modown/timthumb.php?src=https%3A%2F%2Fwww.lgych.com%2Fwp-content%2Fuploads%2F2023%2F10%2Fgir.jpg&w=285&h=285&zc=1"
                   alt="" />
            </a>
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response):
            candidates = search_lgych_candidates(
                artist="少女时代",
                title="FOREVER 1",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(candidates, [])

    def test_search_lgych_dedupes_and_skips_noise_images(self) -> None:
        html = """
        <html>
          <body>
            <img class="thumb" data-src="https://www.lgych.com/wp-content/uploads/2020/11/logo.png" alt="logo" />
            <img class="thumb" data-src="https://www.lgych.com/wp-content/uploads/2025/01/cover.jpg" alt="cover A" />
            <img class="thumb" data-src="https://www.lgych.com/wp-content/uploads/2025/01/cover.jpg" alt="cover B" />
          </body>
        </html>
        """
        response = Mock()
        response.text = html
        response.raise_for_status = Mock()

        with patch("mv_scraper.cli.requests.get", return_value=response):
            candidates = search_lgych_candidates(
                artist="Adele",
                title="Hello",
                timeout=8,
                proxy_url=None,
                limit=5,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["artwork_url"], "https://www.lgych.com/wp-content/uploads/2025/01/cover.jpg")

    def test_find_fallback_uses_lgych_when_others_not_found(self) -> None:
        with patch("mv_scraper.cli.search_deezer_candidates", return_value=[]), patch(
            "mv_scraper.cli.search_audiodb_candidates", return_value=[]
        ), patch(
            "mv_scraper.cli.search_lgych_candidates",
            return_value=[{"artwork_url": "https://www.lgych.com/wp-content/uploads/2025/01/fallback.jpg"}],
        ):
            poster_url, source = find_fallback_poster_url("Adele", "Hello", timeout=8, proxy_url=None)

        self.assertEqual(source, "lgych.com")
        self.assertEqual(poster_url, "https://www.lgych.com/wp-content/uploads/2025/01/fallback.jpg")

    def test_find_fallback_prefers_lgych_over_other_platforms(self) -> None:
        with patch(
            "mv_scraper.cli.search_lgych_candidates",
            return_value=[{"artwork_url": "https://www.lgych.com/wp-content/uploads/2025/01/lgych-first.jpg"}],
        ), patch(
            "mv_scraper.cli.search_deezer_candidates",
            return_value=[{"artwork_url": "https://example.com/deezer.jpg"}],
        ), patch(
            "mv_scraper.cli.search_audiodb_candidates",
            return_value=[{"artwork_url": "https://example.com/audiodb.jpg"}],
        ):
            poster_url, source = find_fallback_poster_url("Girls' Generation", "FOREVER 1", timeout=8, proxy_url=None)

        self.assertEqual(source, "lgych.com")
        self.assertEqual(poster_url, "https://www.lgych.com/wp-content/uploads/2025/01/lgych-first.jpg")

    def test_build_metadata_prefers_youtube_thumb(self) -> None:
        parsed = ParsedTrack(artist="Girls' Generation", title="FOREVER 1", raw="Girls' Generation - FOREVER 1")
        itunes = {"artwork_url": "https://example.com/itunes.jpg"}
        youtube = {"thumbnail": "https://i.ytimg.com/vi/abc123/maxresdefault.jpg"}

        metadata = build_metadata(parsed, itunes=itunes, youtube=youtube)
        self.assertEqual(metadata.thumb_url, "https://i.ytimg.com/vi/abc123/maxresdefault.jpg")

    def test_build_metadata_sets_rating_from_youtube_popularity(self) -> None:
        parsed = ParsedTrack(artist="Adele", title="Hello", raw="Adele - Hello")
        itunes = {"track_name": "Hello", "collection_name": "25"}
        youtube = {"view_count": 100_000_000, "like_count": 3_500_000}

        metadata = build_metadata(parsed, itunes=itunes, youtube=youtube)
        self.assertIsNotNone(metadata.rating)
        self.assertGreaterEqual(metadata.rating or 0, 7.0)
        self.assertLessEqual(metadata.rating or 10, 9.5)
        self.assertIsNotNone(metadata.user_rating)
        self.assertEqual(metadata.votes, 100_000_000)

    def test_write_nfo_includes_rating_fields(self) -> None:
        parsed = ParsedTrack(artist="Adele", title="Hello", raw="Adele - Hello")
        itunes = {"track_name": "Hello", "collection_name": "25"}
        youtube = {"view_count": 10_000_000, "like_count": 500_000}
        metadata = build_metadata(parsed, itunes=itunes, youtube=youtube)

        with tempfile.TemporaryDirectory(prefix="mv_nfo_rating_") as tmp:
            nfo_path = Path(tmp) / "Adele - Hello.nfo"
            write_nfo(metadata, nfo_path, poster_file_name="Adele - Hello-poster.jpg")
            root = ET.parse(nfo_path).getroot()

        self.assertIsNotNone(root.findtext("rating"))
        self.assertIsNotNone(root.findtext("userrating"))
        self.assertIsNotNone(root.findtext("votes"))

    def test_clean_youtube_description_for_plot_removes_links_tracklist_hashtags(self) -> None:
        description = """
        Girls' Generation's 7th Album "FOREVER 1" is out!
        Listen and download on your favorite platforms: https://GirlsGeneration.lnk.to/FOREVER1

        [Tracklist]
        01 FOREVER 1
        #GirlsGeneration #FOREVER1
        """
        cleaned = clean_youtube_description_for_plot(description)
        self.assertIn("7th Album", cleaned)
        self.assertNotIn("https://", cleaned)
        self.assertNotIn("#GirlsGeneration", cleaned)
        self.assertNotIn("Tracklist", cleaned)

    def test_build_plot_text_fallback_is_not_raw_description(self) -> None:
        parsed = ParsedTrack(artist="Girls' Generation", title="FOREVER 1", raw="Girls' Generation - FOREVER 1")
        itunes = {
            "artist_name": "Girls' Generation",
            "track_name": "FOREVER 1",
            "collection_name": "FOREVER 1 - The 7th Album",
            "release_date": "2022-08-05",
        }
        youtube = {
            "channel": "SMTOWN",
            "description": 'Girls\' Generation\'s 7th Album "FOREVER 1" is out! https://example.com #FOREVER1',
        }

        plot = build_plot_text(
            parsed=parsed,
            itunes=itunes,
            youtube=youtube,
            timeout=8,
            proxy_url=None,
            ai_provider="openai",
            ai_api_key=None,
            ai_model=DEFAULT_OPENAI_MODEL,
            ai_base_url="https://api.openai.com/v1",
        )
        self.assertIsNotNone(plot)
        self.assertIn("FOREVER 1", plot or "")
        self.assertNotIn("https://", plot or "")
        self.assertNotIn("#FOREVER1", plot or "")

    def test_build_plot_text_prefers_ai_when_key_available(self) -> None:
        parsed = ParsedTrack(artist="Adele", title="Hello", raw="Adele - Hello")
        itunes = {"artist_name": "Adele", "track_name": "Hello", "collection_name": "25", "release_date": "2015-10-23"}
        youtube = {"channel": "AdeleVEVO", "description": "Hello is a song from Adele's album 25."}

        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "《Hello》是 Adele 的代表作，MV 由官方频道发布。"
                    }
                }
            ]
        }

        with patch("mv_scraper.cli.requests.post", return_value=mock_response):
            plot = build_plot_text(
                parsed=parsed,
                itunes=itunes,
                youtube=youtube,
                timeout=8,
                proxy_url=None,
                ai_provider="openai",
                ai_api_key="test-key",
                ai_model=DEFAULT_OPENAI_MODEL,
                ai_base_url="https://api.openai.com/v1",
            )

        self.assertEqual(plot, "《Hello》是 Adele 的代表作，MV 由官方频道发布。")

    def test_build_plot_text_uses_internet_intro_when_youtube_missing(self) -> None:
        parsed = ParsedTrack(artist="Girls' Generation", title="FOREVER 1", raw="Girls' Generation - FOREVER 1")
        itunes = {
            "artist_name": "Girls' Generation",
            "track_name": "FOREVER 1",
            "collection_name": "FOREVER 1 - The 7th Album",
            "release_date": "2022-08-05",
        }
        youtube = {"channel": "SMTOWN", "description": ""}

        search_response = Mock()
        search_response.raise_for_status = Mock()
        search_response.json.return_value = {
            "query": {
                "search": [
                    {
                        "title": "FOREVER 1",
                        "snippet": "FOREVER 1 是少女时代的歌曲",
                    }
                ]
            }
        }

        extract_response = Mock()
        extract_response.raise_for_status = Mock()
        extract_response.json.return_value = {
            "query": {
                "pages": [
                    {
                        "title": "FOREVER 1",
                        "extract": "《FOREVER 1》是韩国女子组合少女时代于2022年发行的歌曲，收录于第七张同名专辑。",
                    }
                ]
            }
        }

        ai_response = Mock()
        ai_response.raise_for_status = Mock()
        ai_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "《FOREVER 1》由少女时代演唱，MV由SMTOWN发布并围绕歌曲主题进行视觉化呈现。"
                    }
                }
            ]
        }

        with patch("mv_scraper.cli.requests.get", side_effect=[search_response, extract_response]), patch(
            "mv_scraper.cli.requests.post", return_value=ai_response
        ) as mocked_post:
            plot = build_plot_text(
                parsed=parsed,
                itunes=itunes,
                youtube=youtube,
                timeout=8,
                proxy_url=None,
                ai_provider="openai",
                ai_api_key="test-key",
                ai_model=DEFAULT_OPENAI_MODEL,
                ai_base_url="https://api.openai.com/v1",
            )

        prompt = mocked_post.call_args.kwargs["json"]["messages"][1]["content"]
        self.assertIn("reference_intro_source", prompt)
        self.assertIn("internet", prompt)
        self.assertEqual(plot, "《FOREVER 1》由少女时代演唱，MV由SMTOWN发布并围绕歌曲主题进行视觉化呈现。")


if __name__ == "__main__":
    unittest.main()
