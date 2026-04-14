import unittest
from pathlib import Path
from unittest.mock import patch

from mv_scraper.web import build_poster_candidates


class WebCandidateTests(unittest.TestCase):
    def test_youtube_candidates_filter_irrelevant_results(self) -> None:
        youtube_raw = [
            {
                "title": "Alicia Keys - No One (Official Music Video)",
                "channel": "Alicia Keys",
                "thumbnail": "https://example.com/no-one.jpg",
                "webpage_url": "https://youtube.com/watch?v=no-one",
            },
            {
                "title": "BoA 보아 'Crazier' MV",
                "channel": "SMTOWN",
                "thumbnail": "https://example.com/crazier.jpg",
                "webpage_url": "https://youtube.com/watch?v=crazier",
            },
            {
                "title": "Director Reacts - BoA - Crazier MV",
                "channel": "Roscoe",
                "thumbnail": "https://example.com/reaction.jpg",
                "webpage_url": "https://youtube.com/watch?v=reaction",
            },
        ]

        with patch("mv_scraper.web.search_lgych_candidates", return_value=[]), patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_itunes_candidates", return_value=[]
        ), patch("mv_scraper.web.search_youtube_candidates", return_value=youtube_raw), patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ):
            result = build_poster_candidates(
                video_path=Path("BoA - Crazier (Bugs! 4K).mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        youtube_titles = [item["title"] for item in result["candidates"] if item["source"] == "YouTube"]
        self.assertIn("BoA 보아 'Crazier' MV", youtube_titles)
        self.assertNotIn("Alicia Keys - No One (Official Music Video)", youtube_titles)
        self.assertEqual(youtube_titles[0], "BoA 보아 'Crazier' MV")

    def test_itunes_candidates_prioritize_precise_track(self) -> None:
        itunes_raw = [
            {
                "artist_name": "EXO",
                "track_name": "Forever",
                "collection_name": "THE WAR",
                "artwork_url": "https://example.com/exo.jpg",
            },
            {
                "artist_name": "Girls' Generation",
                "track_name": "FOREVER 1",
                "collection_name": "FOREVER 1 - The 7th Album",
                "artwork_url": "https://example.com/snsd.jpg",
            },
            {
                "artist_name": "Girls' Generation",
                "track_name": "Forever",
                "collection_name": "Oh!",
                "artwork_url": "https://example.com/snsd-forever.jpg",
            },
        ]

        with patch("mv_scraper.web.search_lgych_candidates", return_value=[]), patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_itunes_candidates", return_value=itunes_raw
        ), patch("mv_scraper.web.search_youtube_candidates", return_value=[]), patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ):
            result = build_poster_candidates(
                video_path=Path("Girls' Generation - FOREVER 1.mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        itunes_titles = [item["title"] for item in result["candidates"] if item["source"] == "iTunes"]
        self.assertTrue(any("FOREVER 1" in title for title in itunes_titles))
        self.assertFalse(any("EXO" in title for title in itunes_titles))

    def test_search_uses_simplified_title_without_variant_suffix(self) -> None:
        with patch("mv_scraper.web.search_lgych_candidates", return_value=[]) as mocked_lgych, patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ) as mocked_bugs, patch(
            "mv_scraper.web.search_itunes_candidates", return_value=[]
        ) as mocked_itunes, patch("mv_scraper.web.search_youtube_candidates", return_value=[]) as mocked_youtube, patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ) as mocked_deezer, patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ) as mocked_audiodb:
            build_poster_candidates(
                video_path=Path("ILLIT - Almond Chocolate (Special Film) (Bugs! 4K).mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        self.assertEqual(mocked_youtube.call_args.args[0], "ILLIT Almond Chocolate official music video")
        self.assertEqual(mocked_lgych.call_args.kwargs["title"], "Almond Chocolate")
        self.assertEqual(mocked_bugs.call_args.kwargs["title"], "Almond Chocolate")
        self.assertEqual(mocked_itunes.call_args.kwargs["title"], "Almond Chocolate")
        self.assertEqual(mocked_deezer.call_args.kwargs["title"], "Almond Chocolate")
        self.assertEqual(mocked_audiodb.call_args.kwargs["title"], "Almond Chocolate")

    def test_lgych_candidates_are_visible(self) -> None:
        lgych_raw = [
            {
                "artist_name": "Girls' Generation",
                "track_name": "少女时代 Girl's Generation - FOREVER 1 4K 2160P [Bugs MP4 1.2GB]",
                "collection_name": "lgych.com",
                "artwork_url": "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg",
            }
        ]
        itunes_raw = [
            {
                "artist_name": "Girls' Generation",
                "track_name": "FOREVER 1",
                "collection_name": "FOREVER 1 - The 7th Album",
                "artwork_url": "https://example.com/itunes-snsd.jpg",
            }
        ]

        with patch("mv_scraper.web.search_lgych_candidates", return_value=lgych_raw), patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_itunes_candidates", return_value=itunes_raw
        ), patch("mv_scraper.web.search_youtube_candidates", return_value=[]), patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ):
            result = build_poster_candidates(
                video_path=Path("Girls' Generation - FOREVER 1.mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        self.assertGreaterEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["source"], "lgych.com")
        self.assertFalse(any(item["source"] == "iTunes" for item in result["candidates"]))

    def test_youtube_has_higher_priority_than_lgych(self) -> None:
        lgych_raw = [
            {
                "artist_name": "Girls' Generation",
                "track_name": "少女时代 Girl's Generation - FOREVER 1 4K 2160P",
                "collection_name": "lgych.com",
                "artwork_url": "https://www.lgych.com/wp-content/uploads/2023/10/gir.jpg",
            }
        ]
        youtube_raw = [
            {
                "title": "Girls' Generation - FOREVER 1 MV",
                "channel": "SMTOWN",
                "thumbnail": "https://i.ytimg.com/vi/abc123/maxresdefault.jpg",
                "webpage_url": "https://www.youtube.com/watch?v=abc123",
            }
        ]

        with patch("mv_scraper.web.search_lgych_candidates", return_value=lgych_raw), patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_itunes_candidates", return_value=[]
        ), patch("mv_scraper.web.search_youtube_candidates", return_value=youtube_raw), patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ):
            result = build_poster_candidates(
                video_path=Path("Girls' Generation - FOREVER 1.mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        self.assertGreaterEqual(len(result["candidates"]), 2)
        self.assertEqual(result["candidates"][0]["source"], "YouTube")
        self.assertEqual(result["candidates"][1]["source"], "lgych.com")

    def test_youtube_candidates_prefer_official_mv_before_performance_variants(self) -> None:
        youtube_raw = [
            {
                "title": "ILLIT (아일릿) 'Tick-Tack' Official MV (Performance ver.)",
                "channel": "HYBE LABELS",
                "thumbnail": "https://example.com/performance.jpg",
                "webpage_url": "https://youtube.com/watch?v=performance",
            },
            {
                "title": "ILLIT (아일릿) 'Tick-Tack' Official MV",
                "channel": "HYBE LABELS",
                "thumbnail": "https://example.com/official.jpg",
                "webpage_url": "https://youtube.com/watch?v=official",
            },
            {
                "title": "ILLIT (아일릿) 'Tick-Tack' Dance Practice",
                "channel": "ILLIT",
                "thumbnail": "https://example.com/practice.jpg",
                "webpage_url": "https://youtube.com/watch?v=practice",
            },
        ]

        with patch("mv_scraper.web.search_lgych_candidates", return_value=[]), patch(
            "mv_scraper.web.search_bugs_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_itunes_candidates", return_value=[]
        ), patch("mv_scraper.web.search_youtube_candidates", return_value=youtube_raw), patch(
            "mv_scraper.web.search_deezer_candidates", return_value=[]
        ), patch(
            "mv_scraper.web.search_audiodb_candidates", return_value=[]
        ):
            result = build_poster_candidates(
                video_path=Path("ILLIT - Tick-Tack.mp4"),
                default_artist=None,
                query=None,
                timeout=8,
                proxy_url=None,
            )

        youtube_titles = [item["title"] for item in result["candidates"] if item["source"] == "YouTube"]
        self.assertEqual(youtube_titles[0], "ILLIT (아일릿) 'Tick-Tack' Official MV")
        self.assertCountEqual(
            youtube_titles[1:],
            [
                "ILLIT (아일릿) 'Tick-Tack' Official MV (Performance ver.)",
                "ILLIT (아일릿) 'Tick-Tack' Dance Practice",
            ],
        )


if __name__ == "__main__":
    unittest.main()
