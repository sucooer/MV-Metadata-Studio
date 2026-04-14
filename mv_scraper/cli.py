from __future__ import annotations

import argparse
import html
import logging
import math
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import requests
from PIL import Image, UnidentifiedImageError
from yt_dlp import YoutubeDL

DEFAULT_FANART_API_KEY = "34bfe02ba184d8ddf12e67eec99eb7d2"
DEFAULT_AI_PROVIDER = "openai"
DEFAULT_AI_MODEL = "gpt-4.1-mini"
DEFAULT_OPENAI_MODEL = DEFAULT_AI_MODEL
AI_PROVIDER_PRESETS: dict[str, dict[str, str]] = {
    "openai": {"base_url": "https://api.openai.com/v1", "default_model": "gpt-4.1-mini"},
    "openrouter": {"base_url": "https://openrouter.ai/api/v1", "default_model": "openai/gpt-4.1-mini"},
    "deepseek": {"base_url": "https://api.deepseek.com/v1", "default_model": "deepseek-chat"},
    "siliconflow": {"base_url": "https://api.siliconflow.cn/v1", "default_model": "Qwen/Qwen2.5-7B-Instruct"},
    "custom": {"base_url": "http://127.0.0.1:11434/v1", "default_model": "gpt-4.1-mini"},
}

VIDEO_EXTENSIONS = {
    ".mp4",
    ".mkv",
    ".avi",
    ".mov",
    ".wmv",
    ".flv",
    ".m4v",
    ".webm",
    ".ts",
}

NOISE_KEYWORDS = (
    "official",
    "music video",
    "mv",
    "lyrics",
    "lyric",
    "live",
    "karaoke",
    "4k",
    "1080p",
    "hd",
    "中字",
    "完整版",
    "shorts",
)

PLATFORM_KEYWORDS = (
    "prores",
    "blu-ray",
    "bluray",
    "blu ray",
    "bugs",
    "master",
    "web-dl",
    "webdl",
    "web dl",
    "melon",
    "gomtv",
    "genie",
    "flo",
    "spotify",
    "apple music",
    "youtube music",
    "deezer",
    "tidal",
    "qq music",
    "netease",
    "netease cloud music",
    "line music",
    "linemusic",
    "amazon music",
    "joox",
    "网易云",
    "酷狗",
    "酷我",
    "咪咕",
)

FANART_EXTRA_ARTWORK_TARGETS = (
    ("clearlogo", "png", ("hdmusiclogo", "musiclogo"), False),
    ("logo", "png", ("hdmusiclogo", "musiclogo"), True),
    ("banner", "jpg", ("musicbanner",), False),
    ("fanart", "jpg", ("artistbackground",), False),
    ("landscape", "jpg", ("artistbackground",), True),
    ("thumb", "jpg", ("artistthumb",), False),
)


@dataclass
class ParsedTrack:
    artist: str
    title: str
    raw: str


@dataclass
class CombinedMetadata:
    artist: str
    title: str
    album: Optional[str] = None
    plot: Optional[str] = None
    premiered: Optional[str] = None
    year: Optional[str] = None
    studio: Optional[str] = None
    genre: str = "Music"
    thumb_url: Optional[str] = None
    youtube_url: Optional[str] = None
    youtube_id: Optional[str] = None
    duration_seconds: Optional[int] = None
    tagline: Optional[str] = None
    rating: Optional[float] = None
    user_rating: Optional[int] = None
    votes: Optional[int] = None


@dataclass
class RunStats:
    scanned: int = 0
    success: int = 0
    skipped: int = 0
    failed: int = 0


def normalize_text(text: str) -> str:
    normalized = text.replace("_", " ").replace(".", " ").strip()
    return re.sub(r"\s+", " ", normalized)


def clean_component(text: str) -> str:
    cleaned = normalize_text(text)
    cleaned = cleaned.strip("-_| ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def remove_noise_fragments(text: str) -> str:
    def replace_bracket(match: re.Match[str]) -> str:
        body = match.group(0)[1:-1].strip().lower()
        bracket_noise_keywords = (*NOISE_KEYWORDS, *PLATFORM_KEYWORDS)
        if any(keyword in body for keyword in bracket_noise_keywords):
            return " "
        return match.group(0)

    cleaned = re.sub(r"\[[^\]]+\]|\([^\)]+\)", replace_bracket, text)
    for keyword in NOISE_KEYWORDS:
        cleaned = re.sub(rf"\b{re.escape(keyword)}\b", " ", cleaned, flags=re.IGNORECASE)

    platform_pattern = "|".join(re.escape(keyword) for keyword in PLATFORM_KEYWORDS)
    if platform_pattern:
        suffix_pattern = rf"(?:\s*[-–—|_/]+\s*)?(?:{platform_pattern})\s*$"
        while True:
            next_cleaned = re.sub(suffix_pattern, " ", cleaned, flags=re.IGNORECASE)
            if next_cleaned == cleaned:
                break
            cleaned = next_cleaned

    cleaned = cleaned.strip("-_| ")
    return re.sub(r"\s+", " ", cleaned)


def parse_artist_title(candidate: str) -> Optional[ParsedTrack]:
    raw = candidate
    candidate = normalize_text(candidate)
    patterns = (
        re.compile(r"^\[(?P<artist>[^\]]+)\]\s*(?P<title>.+)$"),
        re.compile(r"^(?P<artist>.+?)\s*[-–—]\s*(?P<title>.+)$"),
        re.compile(r"^(?P<title>.+?)\s+by\s+(?P<artist>.+)$", flags=re.IGNORECASE),
    )

    for pattern in patterns:
        match = pattern.match(candidate)
        if not match:
            continue

        artist = clean_component(match.group("artist"))
        title = remove_noise_fragments(clean_component(match.group("title")))
        if artist and title:
            return ParsedTrack(artist=artist, title=title, raw=raw)
    return None


def infer_track_from_path(video_path: Path, default_artist: Optional[str]) -> Optional[ParsedTrack]:
    candidates = [video_path.stem, video_path.parent.name, f"{video_path.parent.name} - {video_path.stem}"]
    for candidate in candidates:
        parsed = parse_artist_title(candidate)
        if parsed:
            return parsed

    title = remove_noise_fragments(clean_component(video_path.stem))
    if default_artist and title:
        return ParsedTrack(artist=clean_component(default_artist), title=title, raw=video_path.stem)
    return None


def format_date(date_text: Optional[str]) -> Optional[str]:
    if not date_text:
        return None

    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(date_text[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def format_narrative_date(date_text: Optional[str]) -> Optional[str]:
    value = str(date_text or "").strip()
    if not value:
        return None
    try:
        dt = datetime.strptime(value[:10], "%Y-%m-%d")
        return f"{dt.year}年{dt.month}月{dt.day}日"
    except ValueError:
        return value


def upscale_itunes_artwork(url: str) -> str:
    return re.sub(r"/\d+x\d+bb\.", "/1200x1200bb.", url)


def build_requests_kwargs(timeout: int, proxy_url: Optional[str]) -> dict:
    kwargs = {"timeout": timeout}
    if proxy_url:
        kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    return kwargs


def normalize_proxy_url(proxy_url: Optional[str]) -> Optional[str]:
    raw = str(proxy_url or "").strip()
    if not raw:
        return None

    normalized = raw if "://" in raw else f"http://{raw}"
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("proxy must be a valid URL, e.g. http://127.0.0.1:7890")
    return normalized


def resolve_fanart_api_key(fanart_api_key: Optional[str]) -> Optional[str]:
    direct = str(fanart_api_key or "").strip()
    if direct:
        return direct

    env_key = os.getenv("FANART_API_KEY", "").strip()
    if env_key:
        return env_key

    default_key = str(DEFAULT_FANART_API_KEY).strip()
    return default_key or None


def normalize_ai_provider(ai_provider: Optional[str]) -> str:
    raw = str(ai_provider or DEFAULT_AI_PROVIDER).strip().lower()
    if raw in {"", "openai"}:
        return "openai"
    if raw in {"openrouter", "deepseek", "siliconflow", "custom"}:
        return raw
    raise ValueError("ai_provider must be one of: openai, openrouter, deepseek, siliconflow, custom")


def resolve_ai_api_key(ai_provider: str, ai_api_key: Optional[str]) -> Optional[str]:
    direct = str(ai_api_key or "").strip()
    if direct:
        return direct

    provider = normalize_ai_provider(ai_provider)
    env_candidates: tuple[str, ...]
    if provider == "openrouter":
        env_candidates = ("OPENROUTER_API_KEY", "AI_API_KEY")
    elif provider == "deepseek":
        env_candidates = ("DEEPSEEK_API_KEY", "AI_API_KEY")
    elif provider == "siliconflow":
        env_candidates = ("SILICONFLOW_API_KEY", "AI_API_KEY")
    elif provider == "custom":
        env_candidates = ("AI_API_KEY", "OPENAI_API_KEY")
    else:
        env_candidates = ("OPENAI_API_KEY", "AI_API_KEY")

    for env_name in env_candidates:
        env_key = os.getenv(env_name, "").strip()
        if env_key:
            return env_key
    return None


def resolve_ai_base_url(ai_provider: str, ai_base_url: Optional[str]) -> str:
    direct = str(ai_base_url or "").strip()
    if direct:
        return direct.rstrip("/")

    provider = normalize_ai_provider(ai_provider)
    preset = AI_PROVIDER_PRESETS.get(provider, AI_PROVIDER_PRESETS[DEFAULT_AI_PROVIDER])
    return str(preset.get("base_url") or AI_PROVIDER_PRESETS[DEFAULT_AI_PROVIDER]["base_url"]).rstrip("/")


def resolve_ai_model(ai_provider: str, ai_model: Optional[str]) -> str:
    direct = str(ai_model or "").strip()
    if direct:
        return direct

    provider = normalize_ai_provider(ai_provider)
    preset = AI_PROVIDER_PRESETS.get(provider, AI_PROVIDER_PRESETS[DEFAULT_AI_PROVIDER])
    model = str(preset.get("default_model") or DEFAULT_AI_MODEL).strip()
    return model or DEFAULT_AI_MODEL


def resolve_openai_api_key(openai_api_key: Optional[str]) -> Optional[str]:
    return resolve_ai_api_key("openai", openai_api_key)


def should_retry_without_proxy(exc: requests.RequestException) -> bool:
    if isinstance(exc, requests.exceptions.ProxyError):
        return True
    message = str(exc).lower()
    return "proxy" in message and ("cannot connect" in message or "proxyerror" in message)


def build_image_request_headers(image_url: str) -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }

    host = urlparse(image_url).netloc.lower()
    if host.endswith("lgych.com"):
        headers["Referer"] = "https://www.lgych.com/"
        headers["Origin"] = "https://www.lgych.com"
    return headers


def build_lgych_search_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.lgych.com/",
        "Origin": "https://www.lgych.com",
    }


def build_lgych_timthumb_url(image_url: str) -> Optional[str]:
    parsed = urlparse(image_url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if not host.endswith("lgych.com"):
        return None
    if "timthumb.php" in path:
        return None
    if "/wp-content/uploads/" not in path:
        return None

    encoded = quote(image_url, safe="")
    return (
        "https://www.lgych.com/wp-content/themes/modown/timthumb.php"
        f"?src={encoded}&w=1200&h=1200&zc=1&q=95&a=c"
    )


def iter_download_urls(image_url: str) -> list[str]:
    urls = [image_url]
    timthumb_url = build_lgych_timthumb_url(image_url)
    if timthumb_url and timthumb_url not in urls:
        urls.append(timthumb_url)
    return urls


def search_itunes_candidates(
    artist: str, title: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict]:
    params = {
        "term": f"{artist} {title}",
        "entity": "song",
        "limit": max(1, min(limit, 20)),
    }
    headers = {"User-Agent": "mv-emby-scraper/0.1"}
    response = requests.get(
        "https://itunes.apple.com/search",
        params=params,
        headers=headers,
        **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
    )
    response.raise_for_status()
    payload = response.json()

    if payload.get("resultCount", 0) == 0:
        return []

    items: list[dict] = []
    for result in payload.get("results", []):
        artwork = result.get("artworkUrl100")
        items.append(
            {
                "artist_name": result.get("artistName"),
                "track_name": result.get("trackName"),
                "collection_name": result.get("collectionName"),
                "release_date": format_date(result.get("releaseDate")),
                "artwork_url": upscale_itunes_artwork(artwork) if artwork else None,
            }
        )
    return items


def fetch_itunes_metadata(artist: str, title: str, timeout: int, proxy_url: Optional[str] = None) -> dict:
    candidates = search_itunes_candidates(artist, title, timeout=timeout, proxy_url=proxy_url, limit=1)
    return candidates[0] if candidates else {}


def search_deezer_candidates(
    artist: str, title: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict]:
    safe_limit = max(1, min(limit, 25))
    query = f'artist:"{artist}" track:"{title}"' if artist else title
    response = requests.get(
        "https://api.deezer.com/search",
        params={"q": query, "limit": safe_limit},
        **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
    )
    response.raise_for_status()

    payload = response.json()
    data = payload.get("data", [])
    if not data:
        return []

    items: list[dict] = []
    for entry in data:
        album = entry.get("album") or {}
        artist_info = entry.get("artist") or {}
        image_url = album.get("cover_xl") or album.get("cover_big") or album.get("cover_medium")
        if not image_url:
            continue

        items.append(
            {
                "artist_name": artist_info.get("name"),
                "track_name": entry.get("title"),
                "collection_name": album.get("title"),
                "release_date": format_date(entry.get("release_date")),
                "artwork_url": image_url,
            }
        )
    return items


def search_audiodb_candidates(
    artist: str, title: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict]:
    params = {"t": title}
    if artist:
        params["s"] = artist

    response = requests.get(
        "https://www.theaudiodb.com/api/v1/json/2/searchtrack.php",
        params=params,
        **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
    )
    response.raise_for_status()

    payload = response.json()
    tracks = payload.get("track")
    if not tracks:
        return []

    items: list[dict] = []
    for entry in tracks[: max(1, min(limit, 25))]:
        image_url = entry.get("strTrackThumb") or entry.get("strAlbumThumb")
        if not image_url:
            continue

        items.append(
            {
                "artist_name": entry.get("strArtist"),
                "track_name": entry.get("strTrack"),
                "collection_name": entry.get("strAlbum"),
                "release_date": format_date(entry.get("intYearReleased")),
                "artwork_url": image_url,
            }
        )
    return items


def normalize_artist_identity(text: Optional[str]) -> str:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return ""
    normalized = normalized.replace("’", "'").replace("‘", "'").replace("`", "'")
    normalized = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def artist_token_overlap_ratio(expected: str, actual: str) -> float:
    expected_tokens = {token for token in normalize_artist_identity(expected).split(" ") if token}
    actual_tokens = {token for token in normalize_artist_identity(actual).split(" ") if token}
    if not expected_tokens:
        return 0.0
    return len(expected_tokens & actual_tokens) / len(expected_tokens)


def search_musicbrainz_artist_candidates(
    artist: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict]:
    artist_name = str(artist or "").strip()
    if not artist_name:
        return []

    safe_limit = max(1, min(limit, 10))
    response = requests.get(
        "https://musicbrainz.org/ws/2/artist/",
        params={"query": f'artist:"{artist_name}"', "fmt": "json", "limit": safe_limit},
        headers={"User-Agent": "mv-emby-scraper/0.1 (+https://example.com)"},
        **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
    )
    response.raise_for_status()
    payload = response.json()

    items: list[dict] = []
    for entry in payload.get("artists", []):
        mbid = str(entry.get("id") or "").strip()
        name = str(entry.get("name") or "").strip()
        if not mbid or not name:
            continue
        try:
            score = int(entry.get("score", 0))
        except (TypeError, ValueError):
            score = 0
        items.append({"mbid": mbid, "name": name, "score": score})
    return items


def search_fanart_candidates(
    artist: str,
    title: str,
    timeout: int,
    proxy_url: Optional[str] = None,
    limit: int = 5,
    fanart_api_key: Optional[str] = None,
) -> list[dict]:
    api_key = resolve_fanart_api_key(fanart_api_key)
    if not api_key:
        return []

    mb_candidates = search_musicbrainz_artist_candidates(artist=artist, timeout=timeout, proxy_url=proxy_url, limit=8)
    if not mb_candidates:
        return []
    target_artist = normalize_artist_identity(artist)

    def mb_sort_key(item: dict[str, Any]) -> tuple[int, float, int]:
        candidate_name = normalize_artist_identity(item.get("name"))
        score = int(item.get("score", 0))
        exact_match = 1 if target_artist and candidate_name == target_artist else 0
        contains_match = 1 if target_artist and not exact_match and target_artist in candidate_name else 0
        reverse_contains = 1 if target_artist and not exact_match and candidate_name in target_artist else 0
        overlap = artist_token_overlap_ratio(target_artist, candidate_name)
        return exact_match + contains_match + reverse_contains, overlap, score

    mb_candidates.sort(key=mb_sort_key, reverse=True)
    mb_candidates = mb_candidates[:3]

    safe_limit = max(1, min(limit, 20))
    image_keys = (
        ("artistthumb", "artistthumb"),
        ("artistbackground", "artistbackground"),
        ("musicbanner", "musicbanner"),
        ("musiclogo", "musiclogo"),
        ("hdmusiclogo", "hdmusiclogo"),
    )

    items: list[dict] = []
    seen_urls: set[str] = set()
    for artist_entry in mb_candidates:
        mbid = artist_entry["mbid"]
        artist_name = artist_entry["name"]
        try:
            response = requests.get(
                f"https://webservice.fanart.tv/v3/music/{mbid}",
                params={"api_key": api_key},
                headers={"User-Agent": "mv-emby-scraper/0.1"},
                **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
            )
            if response.status_code == 404:
                continue
            response.raise_for_status()
        except requests.RequestException:
            continue

        payload = response.json()
        for image_key, image_type in image_keys:
            values = payload.get(image_key)
            if not isinstance(values, list):
                continue
            for entry in values:
                image_url = str((entry or {}).get("url") or "").strip()
                if not image_url:
                    continue
                lowered = image_url.lower()
                if lowered in seen_urls:
                    continue
                seen_urls.add(lowered)
                items.append(
                    {
                        "artist_name": artist_name,
                        "track_name": title or None,
                        "collection_name": f"fanart.tv ({image_type})",
                        "artwork_type": image_type,
                        "release_date": None,
                        "artwork_url": image_url,
                    }
                )
                if len(items) >= safe_limit:
                    return items
    return items


def extract_html_attr(tag: str, attr: str) -> Optional[str]:
    pattern = rf"""\b{re.escape(attr)}\s*=\s*(?P<q>["'])(?P<value>.*?)(?P=q)"""
    match = re.search(pattern, tag, flags=re.IGNORECASE)
    if not match:
        return None
    value = html.unescape(match.group("value")).strip()
    return value or None


class LgychSearchParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchor_title_stack: list[str] = []
        self.entries: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attr_map = {str(key).lower(): (value or "") for key, value in attrs}
        tag_name = tag.lower()

        if tag_name == "a":
            self.anchor_title_stack.append(attr_map.get("title", "").strip())
            return

        if tag_name != "img":
            return

        class_value = attr_map.get("class", "").lower()
        classes = {part for part in re.split(r"\s+", class_value) if part}
        if "thumb" not in classes:
            return

        image_source = (attr_map.get("data-src") or attr_map.get("src") or "").strip()
        if not image_source:
            return

        anchor_title = self.anchor_title_stack[-1] if self.anchor_title_stack else ""
        image_alt = attr_map.get("alt", "").strip()
        title_text = anchor_title or image_alt
        self.entries.append((image_source, title_text))

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self.anchor_title_stack:
            self.anchor_title_stack.pop()

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        self.handle_starttag(tag, attrs)


def sanitize_lgych_title(raw_title: str) -> str:
    title = html.unescape(str(raw_title or "")).strip()
    title = re.sub(r"\s+", " ", title)
    if not title:
        return ""

    lowered = title.lower()
    if "<" in title or ">" in title:
        return ""
    if any(fragment in lowered for fragment in ("<img", "</a", "</div", " class=", " href=")):
        return ""
    return title


def parse_lgych_search_entries(html_text: str) -> list[tuple[str, str]]:
    parser = LgychSearchParser()
    parser.feed(html_text)
    parser.close()
    return parser.entries


def normalize_lgych_image_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None

    cleaned = raw_url.strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"

    absolute = urljoin("https://www.lgych.com/", cleaned)
    parsed = urlparse(absolute)
    if "timthumb.php" in parsed.path.lower():
        src_values = parse_qs(parsed.query).get("src")
        if src_values:
            original = unquote(src_values[0]).strip()
            if original:
                if original.startswith("//"):
                    original = f"https:{original}"
                return urljoin("https://www.lgych.com/", original)
    return absolute


def normalize_bugs_image_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None

    cleaned = raw_url.strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"

    absolute = urljoin("https://music.bugs.co.kr/", cleaned)
    absolute = re.sub(r"/album/images/\d+/", "/album/images/1000/", absolute)
    return absolute


def parse_bugs_search_tracks(html_text: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    row_pattern = re.compile(r"<tr\b[^>]*rowType=\"track\"[^>]*>.*?</tr>", flags=re.IGNORECASE | re.DOTALL)

    for row_html in row_pattern.findall(html_text):
        mv_id_match = re.search(r'\bmvId="([^"]+)"', row_html, flags=re.IGNORECASE)
        if not mv_id_match or mv_id_match.group(1).strip() in {"", "0"}:
            continue

        track_id_match = re.search(r'\btrackId="([^"]+)"', row_html, flags=re.IGNORECASE)
        image_match = re.search(r'<img[^>]+src="([^"]+)"', row_html, flags=re.IGNORECASE)
        title_match = re.search(
            r'<p\s+class="title"[^>]*>.*?<a[^>]+title="([^"]+)"',
            row_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        artist_match = re.search(
            r'<p\s+class="artist"[^>]*>.*?<a[^>]+title="([^"]+)"',
            row_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        album_match = re.search(r'class="album"\s+title="([^"]+)"', row_html, flags=re.IGNORECASE)

        image_url = normalize_bugs_image_url(html.unescape(image_match.group(1))) if image_match else None
        track_name = clean_component(html.unescape(title_match.group(1))) if title_match else ""
        artist_name = clean_component(html.unescape(artist_match.group(1))) if artist_match else ""
        collection_name = clean_component(html.unescape(album_match.group(1))) if album_match else "Bugs"
        track_id = track_id_match.group(1).strip() if track_id_match else ""
        mv_id = mv_id_match.group(1).strip()

        if not image_url or not track_name:
            continue

        items.append(
            {
                "artist_name": artist_name or None,
                "track_name": track_name,
                "collection_name": collection_name or "Bugs",
                "release_date": None,
                "artwork_url": image_url,
                "track_id": track_id or None,
                "mv_id": mv_id,
                "webpage_url": f"https://music.bugs.co.kr/track/{track_id}" if track_id else None,
            }
        )

    return items


def search_bugs_candidates(
    artist: str, title: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 20))
    query = " ".join(part.strip() for part in (artist, title) if part and str(part).strip())
    if not query:
        return []

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://music.bugs.co.kr/",
        "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
    }
    try:
        response = requests.get(
            "https://music.bugs.co.kr/search/integrated",
            params={"q": query},
            headers=headers,
            **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        if proxy_url and should_retry_without_proxy(exc):
            response = requests.get(
                "https://music.bugs.co.kr/search/integrated",
                params={"q": query},
                headers=headers,
                **build_requests_kwargs(timeout=timeout, proxy_url=None),
            )
            response.raise_for_status()
        else:
            raise

    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for item in parse_bugs_search_tracks(response.text):
        image_url = str(item.get("artwork_url") or "").strip().lower()
        if not image_url or image_url in seen_urls:
            continue
        seen_urls.add(image_url)
        items.append(item)
        if len(items) >= safe_limit:
            break

    return items


def search_lgych_candidates(
    artist: str, title: str, timeout: int, proxy_url: Optional[str] = None, limit: int = 5
) -> list[dict]:
    safe_limit = max(1, min(limit, 25))
    query = " ".join(part.strip() for part in (artist, title) if part and str(part).strip())
    if not query:
        return []

    headers = build_lgych_search_headers()
    try:
        response = requests.get(
            "https://www.lgych.com/",
            params={"s": query},
            headers=headers,
            **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        if proxy_url and should_retry_without_proxy(exc):
            response = requests.get(
                "https://www.lgych.com/",
                params={"s": query},
                headers=headers,
                **build_requests_kwargs(timeout=timeout, proxy_url=None),
            )
            response.raise_for_status()
        else:
            raise

    items: list[dict] = []
    seen_urls: set[str] = set()
    entries = parse_lgych_search_entries(response.text)

    for raw_image, raw_title in entries:
        image_url = normalize_lgych_image_url(raw_image)
        if not image_url:
            continue

        title_text = sanitize_lgych_title(raw_title)
        if not title_text:
            continue

        lowered = image_url.lower()
        if "lgych.com" not in lowered and "/wp-content/uploads/" not in lowered:
            continue
        if any(noise in lowered for noise in ("thumb-ing.gif", "/logo", "cropped-", "weixin", "qrcode", "qr-")):
            continue
        if lowered in seen_urls:
            continue

        seen_urls.add(lowered)
        items.append(
            {
                "artist_name": None,
                "track_name": title_text,
                "collection_name": "lgych.com",
                "release_date": None,
                "artwork_url": image_url,
            }
        )

        if len(items) >= safe_limit:
            break

    return items


def find_fallback_poster_url(
    artist: str,
    title: str,
    timeout: int,
    proxy_url: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    providers = [
        ("lgych.com", search_lgych_candidates),
        ("Bugs", search_bugs_candidates),
        ("Deezer", search_deezer_candidates),
        ("AudioDB", search_audiodb_candidates),
    ]

    for source_name, provider in providers:
        try:
            results = provider(artist, title, timeout=timeout, proxy_url=proxy_url, limit=3)
        except Exception:  # pragma: no cover - network edge
            continue
        if results and results[0].get("artwork_url"):
            return str(results[0]["artwork_url"]), source_name

    return None, None


def search_youtube_candidates(query: str, proxy_url: Optional[str] = None, max_results: int = 1) -> list[dict]:
    safe_max_results = max(1, min(max_results, 20))

    class _QuietLogger:
        def debug(self, _message: str) -> None:
            return

        def warning(self, _message: str) -> None:
            return

        def error(self, _message: str) -> None:
            return

    ydl_options = {
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
        "skip_download": True,
        "extract_flat": False,
        "default_search": f"ytsearch{safe_max_results}",
        "noplaylist": True,
        "logger": _QuietLogger(),
    }
    if proxy_url:
        ydl_options["proxy"] = proxy_url

    with YoutubeDL(ydl_options) as ydl:
        result = ydl.extract_info(query, download=False)

    if not result:
        return []

    entries = result["entries"] if "entries" in result and result["entries"] else [result]
    candidates: list[dict] = []
    for entry in entries:
        if not entry:
            continue
        candidates.append(
            {
                "video_id": entry.get("id"),
                "title": entry.get("title"),
                "description": entry.get("description"),
                "channel": entry.get("channel") or entry.get("uploader"),
                "upload_date": format_date(entry.get("upload_date")),
                "duration": entry.get("duration"),
                "thumbnail": entry.get("thumbnail"),
                "view_count": entry.get("view_count"),
                "like_count": entry.get("like_count"),
                "webpage_url": entry.get("webpage_url") or entry.get("url"),
            }
        )
        if len(candidates) >= safe_max_results:
            break
    return candidates


def fetch_youtube_metadata(query: str, proxy_url: Optional[str] = None) -> dict:
    candidates = search_youtube_candidates(query, proxy_url=proxy_url, max_results=1)
    return candidates[0] if candidates else {}


def estimate_track_rating(itunes: dict, youtube: dict) -> tuple[Optional[float], Optional[int], Optional[int]]:
    view_count_raw = youtube.get("view_count")
    like_count_raw = youtube.get("like_count")

    try:
        view_count = int(view_count_raw) if view_count_raw is not None else None
    except (TypeError, ValueError):
        view_count = None

    try:
        like_count = int(like_count_raw) if like_count_raw is not None else None
    except (TypeError, ValueError):
        like_count = None

    if view_count is None or view_count <= 0:
        return None, None, None

    # Map public attention roughly to a 5.0-9.5 score.
    log_views = math.log10(max(1, view_count))
    rating = 5.2 + max(0.0, min(3.8, (log_views - 3.0) * 0.62))

    if like_count and like_count > 0:
        ratio = like_count / view_count
        if ratio >= 0.04:
            rating += 0.2
        elif ratio < 0.01:
            rating -= 0.2

    if str(itunes.get("track_name") or "").strip():
        rating += 0.1
    if str(itunes.get("collection_name") or "").strip():
        rating += 0.1

    rating = round(max(5.0, min(9.5, rating)), 1)
    user_rating = int(max(0, min(10, round(rating))))
    return rating, user_rating, view_count


def build_metadata(parsed: ParsedTrack, itunes: dict, youtube: dict) -> CombinedMetadata:
    artist = itunes.get("artist_name") or parsed.artist or youtube.get("channel") or "Unknown Artist"
    title = itunes.get("track_name") or parsed.title or youtube.get("title") or "Unknown Title"
    title = remove_noise_fragments(title)

    premiered = itunes.get("release_date") or youtube.get("upload_date")
    year = premiered[:4] if premiered else None

    plot = youtube.get("description")
    if plot:
        plot = plot.strip()

    rating, user_rating, votes = estimate_track_rating(itunes=itunes, youtube=youtube)

    return CombinedMetadata(
        artist=artist,
        title=title,
        album=itunes.get("collection_name"),
        plot=plot,
        premiered=premiered,
        year=year,
        studio=youtube.get("channel"),
        thumb_url=youtube.get("thumbnail") or itunes.get("artwork_url"),
        youtube_url=youtube.get("webpage_url"),
        youtube_id=youtube.get("video_id"),
        duration_seconds=youtube.get("duration"),
        tagline=youtube.get("title"),
        rating=rating,
        user_rating=user_rating,
        votes=votes,
    )


def clean_youtube_description_for_plot(text: Optional[str]) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw.strip():
        return ""

    blocked_markers = (
        "listen and download",
        "official",
        "instagram.com",
        "twitter.com",
        "facebook.com",
        "tiktok.com",
        "youtube.com",
    )
    lines: list[str] = []
    for line in raw.split("\n"):
        current = line.strip()
        if not current:
            continue
        lowered = current.lower()
        if lowered.startswith(("#", "http://", "https://")):
            continue
        if lowered.startswith("[tracklist]") or lowered.startswith("tracklist"):
            break
        if any(marker in lowered for marker in blocked_markers):
            continue
        if re.match(r"^\d{1,2}\s+.+$", current):
            continue
        if "℗" in current:
            continue
        lines.append(current)

    text_block = " ".join(lines)
    text_block = re.sub(r"https?://\S+", " ", text_block, flags=re.IGNORECASE)
    text_block = re.sub(r"#\S+", " ", text_block)
    text_block = re.sub(r"\s+", " ", text_block).strip()
    return text_block


def clean_external_intro_for_plot(text: Optional[str]) -> str:
    value = html.unescape(str(text or ""))
    if not value.strip():
        return ""

    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[[^\]]+\]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"https?://\S+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"#\S+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:420].strip()


def has_cjk_text(text: Optional[str]) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def request_json_with_proxy_retry(
    url: str,
    timeout: int,
    proxy_url: Optional[str],
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
) -> dict:
    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}
    except requests.RequestException as exc:
        if proxy_url and should_retry_without_proxy(exc):
            response = requests.get(
                url,
                params=params,
                headers=headers,
                **build_requests_kwargs(timeout=timeout, proxy_url=None),
            )
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        raise


def fetch_wikipedia_page_extract(
    language: str,
    page_title: str,
    timeout: int,
    proxy_url: Optional[str],
) -> Optional[str]:
    payload = request_json_with_proxy_retry(
        f"https://{language}.wikipedia.org/w/api.php",
        params={
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "redirects": 1,
            "prop": "extracts",
            "exintro": 1,
            "explaintext": 1,
            "titles": page_title,
        },
        headers={"User-Agent": "mv-emby-scraper/0.1"},
        timeout=timeout,
        proxy_url=proxy_url,
    )
    pages = ((payload.get("query") or {}).get("pages") or []) if isinstance(payload, dict) else []
    for page in pages:
        if not isinstance(page, dict):
            continue
        extract = clean_external_intro_for_plot(page.get("extract"))
        if len(extract) >= 24:
            return extract
    return None


def fetch_wikipedia_intro(
    artist: str,
    title: str,
    timeout: int,
    proxy_url: Optional[str],
) -> Optional[str]:
    artist_text = str(artist or "").strip()
    title_text = remove_noise_fragments(str(title or "").strip())
    if not title_text:
        return None

    query = " ".join(part for part in (artist_text, title_text, "song") if part)
    headers = {"User-Agent": "mv-emby-scraper/0.1"}

    for language in ("zh", "en"):
        try:
            payload = request_json_with_proxy_retry(
                f"https://{language}.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "format": "json",
                    "list": "search",
                    "srsearch": query,
                    "srlimit": 3,
                },
                headers=headers,
                timeout=timeout,
                proxy_url=proxy_url,
            )
        except requests.RequestException:
            continue

        search_items = ((payload.get("query") or {}).get("search") or []) if isinstance(payload, dict) else []
        for item in search_items:
            if not isinstance(item, dict):
                continue
            page_title = str(item.get("title") or "").strip()
            if not page_title:
                continue

            extract = fetch_wikipedia_page_extract(language, page_title, timeout=timeout, proxy_url=proxy_url)
            if extract:
                return extract

            snippet = clean_external_intro_for_plot(item.get("snippet"))
            if len(snippet) >= 24:
                return snippet
    return None


def fetch_external_intro_for_plot(
    parsed: ParsedTrack,
    itunes: dict,
    youtube: dict,
    timeout: int,
    proxy_url: Optional[str],
) -> Optional[str]:
    artist = str(itunes.get("artist_name") or parsed.artist or youtube.get("channel") or "").strip()
    title = remove_noise_fragments(
        str(itunes.get("track_name") or parsed.title or youtube.get("title") or "").strip()
    )
    if not title:
        return None

    intro = fetch_wikipedia_intro(artist=artist, title=title, timeout=timeout, proxy_url=proxy_url)
    if intro:
        return intro
    return None


def sanitize_generated_plot(text: Optional[str]) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    value = re.sub(r"https?://\S+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"#\S+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:420].strip()


def build_template_plot(
    parsed: ParsedTrack,
    itunes: dict,
    youtube: dict,
    external_intro: Optional[str] = None,
) -> str:
    artist = str(itunes.get("artist_name") or parsed.artist or "").strip()
    title = remove_noise_fragments(str(itunes.get("track_name") or parsed.title or "").strip())
    album = str(itunes.get("collection_name") or "").strip()
    premiered = format_date(str(itunes.get("release_date") or youtube.get("upload_date") or "").strip())
    premiered_narrative = format_narrative_date(premiered)
    studio = str(youtube.get("channel") or "").strip()
    parts: list[str] = []
    if artist and title and premiered_narrative:
        parts.append(f"《{title}》是{artist}于{premiered_narrative}发布的音乐作品。")
    elif artist and title:
        parts.append(f"《{title}》是{artist}演唱的音乐作品。")
    elif title:
        parts.append(f"《{title}》是一首音乐作品。")

    if album:
        parts.append(f"该曲收录于专辑《{album}》。")

    if studio:
        parts.append(f"官方音乐视频由{studio}发布，用于呈现歌曲的视觉表达。")
    elif premiered_narrative and not (artist and title):
        parts.append(f"相关音乐视频公开时间为{premiered_narrative}。")

    external_intro_clean = clean_external_intro_for_plot(external_intro)
    if external_intro_clean and has_cjk_text(external_intro_clean):
        parts.append(external_intro_clean[:150].strip())

    return sanitize_generated_plot(" ".join(part for part in parts if part))


def extract_openai_output_text(payload: dict) -> str:
    output_text = str(payload.get("output_text") or "").strip()
    if output_text:
        return output_text

    chunks: list[str] = []
    for output_item in payload.get("output", []) or []:
        if not isinstance(output_item, dict):
            continue
        for content_item in output_item.get("content", []) or []:
            if not isinstance(content_item, dict):
                continue
            text_value = str(content_item.get("text") or "").strip()
            if text_value:
                chunks.append(text_value)
    return "\n".join(chunks).strip()


def extract_chat_completions_text(payload: dict) -> str:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""

    first = choices[0]
    if not isinstance(first, dict):
        return ""
    message = first.get("message")
    if not isinstance(message, dict):
        return ""

    content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text_value = str(item.get("text") or "").strip()
            if text_value:
                chunks.append(text_value)
        return "\n".join(chunks).strip()
    return ""


def extract_ai_output_text(payload: dict) -> str:
    output = extract_openai_output_text(payload)
    if output:
        return output
    return extract_chat_completions_text(payload)


def build_ai_completion_url(ai_base_url: str) -> str:
    base = str(ai_base_url or "").strip().rstrip("/")
    lowered = base.lower()
    if lowered.endswith("/chat/completions") or lowered.endswith("/responses"):
        return base
    return f"{base}/chat/completions"


def generate_ai_plot(
    parsed: ParsedTrack,
    itunes: dict,
    youtube: dict,
    timeout: int,
    proxy_url: Optional[str],
    ai_provider: str,
    ai_api_key: Optional[str],
    ai_model: str,
    ai_base_url: str,
    reference_intro: Optional[str],
    reference_intro_source: str,
) -> str:
    context = {
        "artist": itunes.get("artist_name") or parsed.artist,
        "title": remove_noise_fragments(str(itunes.get("track_name") or parsed.title or "")),
        "album": itunes.get("collection_name"),
        "release_date": itunes.get("release_date") or youtube.get("upload_date"),
        "studio": youtube.get("channel"),
        "youtube_title": youtube.get("title"),
        "youtube_description_clean": clean_youtube_description_for_plot(youtube.get("description")),
        "reference_intro_source": reference_intro_source,
        "reference_intro": clean_external_intro_for_plot(reference_intro),
    }

    prompt = (
        "请为 Emby musicvideo 的 <plot> 生成中文简介，定位为“MV条目简介”。"
        "写作要求："
        "1) 2-4句，语气自然、专业，避免生硬模板句；"
        "2) 优先交代歌手、歌曲、发行时间、专辑/版本等确定信息；"
        "3) 必须包含至少一句与MV相关的描述（如发布主体、视觉表达、情绪或叙事方向）。"
        "若资料不足，可写“官方MV围绕歌曲主题进行视觉化呈现”，但不要虚构具体镜头；"
        "4) 禁止输出链接、话题标签、宣传口号、平台引流文案；"
        "5) 不要写成“《X》是某某音乐作品”这类句式，直接围绕歌曲与MV本身展开。"
        "6) 当 reference_intro_source=youtube 时，优先参考 YouTube 简介并改写为中文。"
        "当 reference_intro_source=internet 时，表示 YouTube 缺少可用简介，请参考互联网补充资料生成中文简介。"
        f"\n\n可用资料（仅可使用以下事实）：{context}"
    )

    provider = normalize_ai_provider(ai_provider)
    headers = {"Content-Type": "application/json"}
    if ai_api_key:
        headers["Authorization"] = f"Bearer {ai_api_key}"
    if provider == "openrouter":
        headers["HTTP-Referer"] = "https://localhost/mv-scraper"
        headers["X-Title"] = "MV Emby Scraper"

    payload = {
        "model": ai_model,
        "messages": [
            {
                "role": "system",
                "content": "你是资深中文音乐影像编辑，擅长撰写专业、自然的MV简介。严格基于给定事实，不编造信息。",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 260,
    }

    response = requests.post(
        build_ai_completion_url(ai_base_url),
        headers=headers,
        json=payload,
        **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
    )
    response.raise_for_status()
    result = response.json()
    return sanitize_generated_plot(extract_ai_output_text(result))


def build_plot_text(
    parsed: ParsedTrack,
    itunes: dict,
    youtube: dict,
    timeout: int,
    proxy_url: Optional[str],
    ai_provider: str,
    ai_api_key: Optional[str],
    ai_model: str,
    ai_base_url: str,
) -> Optional[str]:
    youtube_intro = clean_youtube_description_for_plot(youtube.get("description"))
    reference_intro = youtube_intro
    reference_intro_source = "youtube"

    if not reference_intro:
        external_intro = fetch_external_intro_for_plot(
            parsed=parsed,
            itunes=itunes,
            youtube=youtube,
            timeout=timeout,
            proxy_url=proxy_url,
        )
        if external_intro:
            reference_intro = external_intro
            reference_intro_source = "internet"
            logging.info("Plot intro fallback source: wikipedia")
    else:
        external_intro = None

    provider = normalize_ai_provider(ai_provider)
    should_try_ai = bool(ai_api_key) or provider == "custom"
    if should_try_ai:
        try:
            ai_plot = generate_ai_plot(
                parsed=parsed,
                itunes=itunes,
                youtube=youtube,
                timeout=timeout,
                proxy_url=proxy_url,
                ai_provider=provider,
                ai_api_key=ai_api_key,
                ai_model=ai_model,
                ai_base_url=ai_base_url,
                reference_intro=reference_intro,
                reference_intro_source=reference_intro_source,
            )
            if ai_plot:
                return ai_plot
        except Exception as exc:  # pragma: no cover - network edge
            logging.warning("AI plot generation failed, fallback to template: %s", exc)

    template_plot = build_template_plot(parsed, itunes, youtube, external_intro=external_intro)
    if template_plot:
        return template_plot

    fallback_raw = reference_intro
    return sanitize_generated_plot(fallback_raw) or None


def add_text_element(parent: ET.Element, tag: str, value: Optional[str]) -> None:
    if value is None:
        return
    text = str(value).strip()
    if not text:
        return
    element = ET.SubElement(parent, tag)
    element.text = text


def write_nfo(metadata: CombinedMetadata, nfo_path: Path, poster_file_name: Optional[str]) -> None:
    root = ET.Element("musicvideo")

    add_text_element(root, "title", metadata.title)
    add_text_element(root, "artist", metadata.artist)
    add_text_element(root, "album", metadata.album)
    add_text_element(root, "plot", metadata.plot)
    add_text_element(root, "tagline", metadata.tagline)
    add_text_element(root, "premiered", metadata.premiered)
    add_text_element(root, "year", metadata.year)
    add_text_element(root, "studio", metadata.studio)
    add_text_element(root, "genre", metadata.genre)
    if metadata.rating is not None:
        add_text_element(root, "rating", f"{metadata.rating:.1f}")
    if metadata.user_rating is not None:
        add_text_element(root, "userrating", str(int(metadata.user_rating)))
    if metadata.votes is not None:
        add_text_element(root, "votes", str(int(metadata.votes)))

    if metadata.duration_seconds:
        add_text_element(root, "runtime", str(max(1, int(round(metadata.duration_seconds / 60)))))
        add_text_element(root, "durationinseconds", str(metadata.duration_seconds))

    if metadata.youtube_url:
        add_text_element(root, "trailer", metadata.youtube_url)

    if metadata.youtube_id:
        unique_id = ET.SubElement(root, "uniqueid", attrib={"type": "youtube", "default": "true"})
        unique_id.text = metadata.youtube_id

    add_text_element(root, "thumb", poster_file_name)
    add_text_element(root, "source", "mv-emby-scraper")

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ", level=0)
    tree.write(nfo_path, encoding="utf-8", xml_declaration=True)


def download_poster(image_url: str, output_path: Path, timeout: int, proxy_url: Optional[str] = None) -> bool:
    last_request_error: Optional[requests.RequestException] = None

    for candidate_url in iter_download_urls(image_url):
        headers = build_image_request_headers(candidate_url)
        try:
            response = requests.get(
                candidate_url,
                headers=headers,
                **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            if proxy_url and should_retry_without_proxy(exc):
                logging.warning("Poster download via proxy failed, retrying direct: %s", exc)
                try:
                    response = requests.get(
                        candidate_url,
                        headers=headers,
                        **build_requests_kwargs(timeout=timeout, proxy_url=None),
                    )
                    response.raise_for_status()
                except requests.RequestException as direct_exc:
                    last_request_error = direct_exc
                    continue
            else:
                last_request_error = exc
                continue

        try:
            image = Image.open(BytesIO(response.content)).convert("RGB")
        except UnidentifiedImageError:
            continue

        output_path.parent.mkdir(parents=True, exist_ok=True)
        image.save(output_path, format="JPEG", quality=92)
        return True

    if last_request_error is not None:
        raise last_request_error
    return False


def resolve_extra_artwork_path(video_path: Path, poster_style: str, artwork_name: str, extension: str) -> Path:
    safe_name = re.sub(r"[^0-9a-zA-Z_-]+", "", artwork_name.strip().lower())
    if not safe_name:
        raise ValueError("artwork_name cannot be empty")

    safe_ext = extension.strip().lower().lstrip(".") or "jpg"
    if poster_style == "folder":
        return video_path.parent / f"{safe_name}.{safe_ext}"
    return video_path.with_name(f"{video_path.stem}-{safe_name}.{safe_ext}")


def download_image_asset(image_url: str, output_path: Path, timeout: int, proxy_url: Optional[str] = None) -> bool:
    last_request_error: Optional[requests.RequestException] = None

    for candidate_url in iter_download_urls(image_url):
        headers = build_image_request_headers(candidate_url)
        try:
            response = requests.get(
                candidate_url,
                headers=headers,
                **build_requests_kwargs(timeout=timeout, proxy_url=proxy_url),
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            if proxy_url and should_retry_without_proxy(exc):
                logging.warning("Artwork download via proxy failed, retrying direct: %s", exc)
                try:
                    response = requests.get(
                        candidate_url,
                        headers=headers,
                        **build_requests_kwargs(timeout=timeout, proxy_url=None),
                    )
                    response.raise_for_status()
                except requests.RequestException as direct_exc:
                    last_request_error = direct_exc
                    continue
            else:
                last_request_error = exc
                continue

        try:
            image = Image.open(BytesIO(response.content))
        except UnidentifiedImageError:
            continue

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() == ".png":
            if image.mode not in ("RGBA", "LA"):
                image = image.convert("RGBA")
            image.save(output_path, format="PNG")
        else:
            image = image.convert("RGB")
            image.save(output_path, format="JPEG", quality=92)
        return True

    if last_request_error is not None:
        raise last_request_error
    return False


def collect_fanart_artwork_urls(
    artist: str,
    title: str,
    timeout: int,
    proxy_url: Optional[str] = None,
    fanart_api_key: Optional[str] = None,
) -> dict[str, list[str]]:
    candidates = search_fanart_candidates(
        artist=artist,
        title=title,
        timeout=timeout,
        proxy_url=proxy_url,
        limit=40,
        fanart_api_key=fanart_api_key,
    )
    urls_by_type: dict[str, list[str]] = {}
    for item in candidates:
        artwork_type = str(item.get("artwork_type") or "").strip().lower()
        artwork_url = str(item.get("artwork_url") or "").strip()
        if not artwork_type or not artwork_url:
            continue
        bucket = urls_by_type.setdefault(artwork_type, [])
        lowered_existing = {value.lower() for value in bucket}
        if artwork_url.lower() not in lowered_existing:
            bucket.append(artwork_url)
    return urls_by_type


def choose_artwork_url(
    urls_by_type: dict[str, list[str]],
    preferred_types: tuple[str, ...],
    used_urls: set[str],
    allow_reuse: bool,
) -> Optional[str]:
    for artwork_type in preferred_types:
        for url in urls_by_type.get(artwork_type, []):
            lowered = url.lower()
            if allow_reuse or lowered not in used_urls:
                return url
    return None


def download_extra_artist_artwork(
    video_path: Path,
    artist: str,
    title: str,
    poster_style: str,
    timeout: int,
    proxy_url: Optional[str] = None,
    overwrite: bool = False,
    fanart_api_key: Optional[str] = None,
) -> dict[str, list[str]]:
    urls_by_type = collect_fanart_artwork_urls(
        artist=artist,
        title=title,
        timeout=timeout,
        proxy_url=proxy_url,
        fanart_api_key=fanart_api_key,
    )
    if not urls_by_type:
        return {"downloaded": [], "existing": [], "failed": []}

    downloaded: list[str] = []
    existing: list[str] = []
    failed: list[str] = []
    used_urls: set[str] = set()

    for artwork_name, extension, preferred_types, allow_reuse in FANART_EXTRA_ARTWORK_TARGETS:
        output_path = resolve_extra_artwork_path(
            video_path=video_path,
            poster_style=poster_style,
            artwork_name=artwork_name,
            extension=extension,
        )
        if output_path.exists() and not overwrite:
            existing.append(str(output_path))
            continue

        chosen_url = choose_artwork_url(
            urls_by_type=urls_by_type,
            preferred_types=preferred_types,
            used_urls=used_urls,
            allow_reuse=allow_reuse,
        )
        if not chosen_url:
            continue

        try:
            if download_image_asset(chosen_url, output_path, timeout=timeout, proxy_url=proxy_url):
                downloaded.append(str(output_path))
                used_urls.add(chosen_url.lower())
            else:
                failed.append(str(output_path))
        except Exception:
            failed.append(str(output_path))

    return {"downloaded": downloaded, "existing": existing, "failed": failed}


def collect_video_files(target: Path, recursive: bool) -> Iterable[Path]:
    if target.is_file():
        if target.suffix.lower() in VIDEO_EXTENSIONS:
            yield target
        return

    if not target.is_dir():
        return

    pattern = "**/*" if recursive else "*"
    for path in sorted(target.glob(pattern)):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            yield path


def resolve_poster_path(video_path: Path, poster_style: str) -> Path:
    if poster_style == "folder":
        return video_path.parent / "poster.jpg"
    return video_path.with_name(f"{video_path.stem}-poster.jpg")


def process_video(video_path: Path, args: argparse.Namespace) -> str:
    nfo_path = video_path.with_suffix(".nfo")
    poster_path = resolve_poster_path(video_path, args.poster_style)
    proxy_url = getattr(args, "proxy", None)
    ai_provider = normalize_ai_provider(getattr(args, "ai_provider", DEFAULT_AI_PROVIDER))
    ai_key_candidate = getattr(args, "ai_api_key", None)
    if not ai_key_candidate:
        ai_key_candidate = getattr(args, "openai_api_key", None)
    ai_model_candidate = getattr(args, "ai_model", None)
    if not ai_model_candidate:
        ai_model_candidate = getattr(args, "openai_model", None)
    ai_base_url_candidate = getattr(args, "ai_base_url", None)

    ai_api_key = resolve_ai_api_key(ai_provider, ai_key_candidate)
    ai_model = resolve_ai_model(ai_provider, ai_model_candidate)
    ai_base_url = resolve_ai_base_url(ai_provider, ai_base_url_candidate)

    if nfo_path.exists() and not args.overwrite:
        logging.info("Skip (NFO already exists): %s", video_path)
        return "skipped"

    parsed = infer_track_from_path(video_path, args.default_artist)
    if not parsed:
        logging.warning("Could not parse artist/title: %s", video_path)
        return "failed"

    query = f"{parsed.artist} {parsed.title} official music video"
    logging.info("Searching metadata for: %s - %s", parsed.artist, parsed.title)

    youtube = {}
    itunes = {}
    try:
        youtube = fetch_youtube_metadata(query, proxy_url=proxy_url)
    except Exception as exc:  # pragma: no cover - network edge
        logging.warning("YouTube search failed for '%s': %s", query, exc)

    try:
        itunes = fetch_itunes_metadata(parsed.artist, parsed.title, args.timeout, proxy_url=proxy_url)
    except Exception as exc:  # pragma: no cover - network edge
        logging.warning("iTunes search failed for '%s - %s': %s", parsed.artist, parsed.title, exc)

    metadata = build_metadata(parsed, itunes, youtube)
    metadata.plot = build_plot_text(
        parsed=parsed,
        itunes=itunes,
        youtube=youtube,
        timeout=args.timeout,
        proxy_url=proxy_url,
        ai_provider=ai_provider,
        ai_api_key=ai_api_key,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
    )
    if not metadata.thumb_url:
        fallback_url, fallback_source = find_fallback_poster_url(
            parsed.artist,
            parsed.title,
            timeout=args.timeout,
            proxy_url=proxy_url,
        )
        if fallback_url:
            metadata.thumb_url = fallback_url
            logging.info("Poster fallback found from %s for %s", fallback_source, video_path.name)

    poster_exists = poster_path.exists()
    poster_written = False
    if metadata.thumb_url and (args.overwrite or not poster_exists):
        try:
            poster_written = download_poster(metadata.thumb_url, poster_path, args.timeout, proxy_url=proxy_url)
            if poster_written:
                logging.info("Poster created: %s", poster_path)
        except Exception as exc:  # pragma: no cover - network edge
            logging.warning("Poster download failed (%s): %s", video_path, exc)

    poster_name_for_nfo = poster_path.name if poster_written or poster_exists else None

    write_nfo(metadata, nfo_path, poster_name_for_nfo)
    logging.info("NFO created: %s", nfo_path)
    return "success"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Emby-compatible MV NFO and poster files from folder/file names."
    )
    parser.add_argument("target", help="Video file or folder path to process")
    parser.add_argument(
        "--recursive",
        dest="recursive",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Recursively scan target folder (default: enabled)",
    )
    parser.add_argument(
        "--default-artist",
        help="Fallback artist when parser can only infer title",
    )
    parser.add_argument(
        "--poster-style",
        choices=("basename", "folder"),
        default="basename",
        help="Poster filename style: basename => <video>-poster.jpg, folder => poster.jpg",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing NFO/poster files",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only parse and search, do not write files",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--proxy",
        help="Optional proxy URL, e.g. http://127.0.0.1:7890 or socks5://127.0.0.1:1080",
    )
    parser.add_argument(
        "--ai-provider",
        choices=("openai", "openrouter", "deepseek", "siliconflow", "custom"),
        default=DEFAULT_AI_PROVIDER,
        help="AI provider for plot generation",
    )
    parser.add_argument(
        "--ai-api-key",
        "--openai-api-key",
        dest="ai_api_key",
        help=(
            "Optional AI API key for plot generation. "
            "Env fallback: OPENAI_API_KEY / OPENROUTER_API_KEY / DEEPSEEK_API_KEY / SILICONFLOW_API_KEY / AI_API_KEY"
        ),
    )
    parser.add_argument(
        "--ai-model",
        "--openai-model",
        dest="ai_model",
        default=None,
        help="AI model name for plot generation",
    )
    parser.add_argument(
        "--ai-base-url",
        default=None,
        help="Override AI API base URL (OpenAI-compatible), e.g. https://api.openai.com/v1",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        args.proxy = normalize_proxy_url(args.proxy)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    args.ai_provider = normalize_ai_provider(getattr(args, "ai_provider", DEFAULT_AI_PROVIDER))
    args.ai_api_key = resolve_ai_api_key(args.ai_provider, getattr(args, "ai_api_key", None))
    args.ai_model = resolve_ai_model(args.ai_provider, getattr(args, "ai_model", None))
    args.ai_base_url = resolve_ai_base_url(args.ai_provider, getattr(args, "ai_base_url", None))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    target = Path(args.target).expanduser().resolve()
    if not target.exists():
        logging.error("Target path does not exist: %s", target)
        return 1

    files = list(collect_video_files(target, args.recursive))
    if not files:
        logging.error("No video files found at target: %s", target)
        return 1

    stats = RunStats(scanned=len(files))

    for video_file in files:
        if args.dry_run:
            parsed = infer_track_from_path(video_file, args.default_artist)
            if parsed:
                logging.info("[DRY-RUN] %s => %s - %s", video_file.name, parsed.artist, parsed.title)
                stats.success += 1
            else:
                logging.warning("[DRY-RUN] Failed to parse: %s", video_file)
                stats.failed += 1
            continue

        status = process_video(video_file, args)
        if status == "success":
            stats.success += 1
        elif status == "skipped":
            stats.skipped += 1
        else:
            stats.failed += 1

    logging.info(
        "Done. scanned=%s success=%s skipped=%s failed=%s",
        stats.scanned,
        stats.success,
        stats.skipped,
        stats.failed,
    )

    if stats.success == 0 and stats.failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
