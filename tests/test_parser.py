import unittest
from pathlib import Path

from mv_scraper.cli import infer_track_from_path, parse_artist_title


class ParserTests(unittest.TestCase):
    def test_parse_dash(self) -> None:
        parsed = parse_artist_title("Adele - Hello (Official MV)")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")

    def test_parse_bracket(self) -> None:
        parsed = parse_artist_title("[Adele] Hello")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")

    def test_parse_by(self) -> None:
        parsed = parse_artist_title("Hello by Adele")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")

    def test_infer_from_parent(self) -> None:
        parsed = infer_track_from_path(Path("Adele - Hello/video.mp4"), default_artist=None)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")

    def test_default_artist(self) -> None:
        parsed = infer_track_from_path(Path("Hello Official MV.mp4"), default_artist="Adele")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")

    def test_parse_removes_platform_suffix(self) -> None:
        parsed = parse_artist_title("Girl's Generation - FOREVER 1- - Bugs")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Girl's Generation")
        self.assertEqual(parsed.title, "FOREVER 1")

    def test_parse_removes_multiple_platform_suffixes(self) -> None:
        parsed = parse_artist_title("Girl's Generation - FOREVER 1 - WEB-DL - ProRes - Blu-Ray - Master - Melon - GomTV")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Girl's Generation")
        self.assertEqual(parsed.title, "FOREVER 1")

    def test_parse_removes_platform_in_brackets(self) -> None:
        parsed = parse_artist_title("Adele - Hello [ProRes MOV 9.26GB]")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.artist, "Adele")
        self.assertEqual(parsed.title, "Hello")


if __name__ == "__main__":
    unittest.main()
