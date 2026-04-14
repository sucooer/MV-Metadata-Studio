import shutil
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import Mock, patch

from mv_scraper.web import app, ensure_nfo_after_manual_apply, normalize_optional_query, sync_nfo_thumb


class FilesApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="mv_web_test_"))
        (self.tmp_dir / "Adele - Hello.mp4").touch()
        self.client = app.test_client()

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_files_endpoint_lists_video(self) -> None:
        response = self.client.get(
            "/api/files",
            query_string={"target": str(self.tmp_dir), "recursive": "true", "poster_style": "basename"},
        )
        self.assertEqual(response.status_code, 200)

        payload = response.get_json()
        self.assertEqual(payload["count"], 1)
        self.assertTrue(payload["files"][0]["parsed"])

    def test_poster_apply_rejects_invalid_url(self) -> None:
        response = self.client.post(
            "/api/poster/apply",
            json={
                "video_path": str(self.tmp_dir / "Adele - Hello.mp4"),
                "image_url": "invalid",
                "poster_style": "basename",
            },
        )
        self.assertEqual(response.status_code, 400)

    def test_sync_nfo_thumb_updates_existing_nfo(self) -> None:
        nfo_path = self.tmp_dir / "Adele - Hello.nfo"
        nfo_path.write_text("<musicvideo><title>Hello - Bugs</title></musicvideo>", encoding="utf-8")
        poster_path = self.tmp_dir / "Adele - Hello-poster.jpg"

        synced = sync_nfo_thumb(self.tmp_dir / "Adele - Hello.mp4", poster_path)
        self.assertTrue(synced)

        root = ET.parse(nfo_path).getroot()
        self.assertEqual(root.findtext("thumb"), poster_path.name)
        self.assertEqual(root.findtext("title"), "Hello")

    def test_proxy_check_requires_proxy(self) -> None:
        response = self.client.post("/api/proxy/check", json={})
        self.assertEqual(response.status_code, 400)

    def test_proxy_check_returns_latency(self) -> None:
        resp1 = Mock()
        resp1.status_code = 200
        resp2 = Mock()
        resp2.status_code = 204
        resp3 = Mock()
        resp3.status_code = 200

        with patch("mv_scraper.web.requests.get", side_effect=[resp1, resp2, resp3]) as mocked_get:
            response = self.client.post(
                "/api/proxy/check",
                json={"proxy": "127.0.0.1:7890", "timeout": 5},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["proxy"], "http://127.0.0.1:7890")
        self.assertEqual(len(payload["checks"]), 3)
        self.assertIsNotNone(payload["latency_ms"])
        self.assertEqual(mocked_get.call_count, 3)

    def test_ensure_nfo_after_manual_apply_writes_minimal_on_failure(self) -> None:
        poster_path = self.tmp_dir / "Adele - Hello-poster.jpg"
        poster_path.write_bytes(b"jpg")

        with patch("mv_scraper.web.process_video", return_value="failed"):
            result = ensure_nfo_after_manual_apply(
                video_path=self.tmp_dir / "Adele - Hello.mp4",
                poster_style="basename",
                timeout=5,
                proxy_url=None,
                default_artist="Adele",
            )

        self.assertTrue(result["nfo_exists"])
        nfo_path = Path(result["nfo_path"])
        self.assertTrue(nfo_path.exists())
        root = ET.parse(nfo_path).getroot()
        self.assertEqual(root.findtext("artist"), "Adele")
        self.assertEqual(root.findtext("title"), "Hello")

    def test_poster_apply_does_not_create_lock_file(self) -> None:
        nfo_path = self.tmp_dir / "Adele - Hello.nfo"
        nfo_path.write_text("<musicvideo><title>Hello</title></musicvideo>", encoding="utf-8")

        with patch("mv_scraper.web.download_poster", return_value=True):
            response = self.client.post(
                "/api/poster/apply",
                json={
                    "video_path": str(self.tmp_dir / "Adele - Hello.mp4"),
                    "image_url": "https://example.com/poster.jpg",
                    "poster_style": "basename",
                    "timeout": 5,
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertNotIn("poster_manual_locked", payload)
        lock_path = self.tmp_dir / "Adele - Hello.poster.lock"
        self.assertFalse(lock_path.exists())

    def test_normalize_optional_query_ignores_placeholder_values(self) -> None:
        self.assertIsNone(normalize_optional_query(None))
        self.assertIsNone(normalize_optional_query(""))
        self.assertIsNone(normalize_optional_query("None"))
        self.assertIsNone(normalize_optional_query(" null "))
        self.assertIsNone(normalize_optional_query("undefined"))
        self.assertEqual(normalize_optional_query("BoA Crazier"), "BoA Crazier")

    def test_poster_search_ignores_string_none_override(self) -> None:
        with patch("mv_scraper.web.build_poster_candidates") as mocked_build:
            mocked_build.return_value = {
                "parsed": {"artist": "Adele", "title": "Hello"},
                "query": "Adele Hello",
                "candidates": [],
            }

            response = self.client.post(
                "/api/poster/search",
                json={
                    "video_path": str(self.tmp_dir / "Adele - Hello.mp4"),
                    "query": "None",
                    "timeout": 5,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(mocked_build.call_args.kwargs["query"])


if __name__ == "__main__":
    unittest.main()
