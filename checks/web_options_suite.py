import unittest

from mv_scraper.web import parse_job_options


class WebOptionTests(unittest.TestCase):
    def test_parse_defaults(self) -> None:
        options = parse_job_options({"target": "./mv"})
        self.assertEqual(options.target, "./mv")
        self.assertTrue(options.recursive)
        self.assertEqual(options.poster_style, "basename")
        self.assertEqual(options.timeout, 20)
        self.assertIsNone(options.proxy)

    def test_invalid_timeout_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_job_options({"target": "./mv", "timeout": 1})

    def test_invalid_poster_style_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_job_options({"target": "./mv", "poster_style": "abc"})

    def test_proxy_without_scheme_normalized(self) -> None:
        options = parse_job_options({"target": "./mv", "proxy": "127.0.0.1:7890"})
        self.assertEqual(options.proxy, "http://127.0.0.1:7890")

    def test_invalid_proxy_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_job_options({"target": "./mv", "proxy": "http://"})

    def test_ai_options_can_be_overridden(self) -> None:
        options = parse_job_options(
            {
                "target": "./mv",
                "ai_provider": "deepseek",
                "ai_api_key": "sk-test",
                "ai_model": "deepseek-chat",
                "ai_base_url": "https://api.deepseek.com/v1",
            }
        )
        self.assertEqual(options.ai_provider, "deepseek")
        self.assertEqual(options.ai_api_key, "sk-test")
        self.assertEqual(options.ai_model, "deepseek-chat")
        self.assertEqual(options.ai_base_url, "https://api.deepseek.com/v1")

    def test_legacy_openai_keys_are_still_accepted(self) -> None:
        options = parse_job_options(
            {"target": "./mv", "openai_api_key": "sk-old", "openai_model": "gpt-4.1-mini"}
        )
        self.assertEqual(options.ai_provider, "openai")
        self.assertEqual(options.ai_api_key, "sk-old")
        self.assertEqual(options.ai_model, "gpt-4.1-mini")


if __name__ == "__main__":
    unittest.main()
