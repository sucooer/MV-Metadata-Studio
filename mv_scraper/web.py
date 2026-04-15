from __future__ import annotations

import argparse
import json
import hashlib
import logging
import re
import threading
import uuid
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, render_template, request

from mv_scraper.cli import (
    CombinedMetadata,
    DEFAULT_AI_PROVIDER,
    DEFAULT_AI_MODEL,
    VIDEO_EXTENSIONS,
    RunStats,
    collect_video_files,
    download_poster,
    infer_track_from_path,
    normalize_ai_provider,
    parse_artist_title,
    process_video,
    resolve_ai_api_key,
    resolve_ai_base_url,
    resolve_ai_model,
    remove_noise_fragments,
    resolve_poster_path,
    search_audiodb_candidates,
    search_bugs_candidates,
    search_deezer_candidates,
    search_itunes_candidates,
    search_lgych_candidates,
    search_youtube_candidates,
    write_nfo,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TARGET = "/media"
PREFERENCES_BASENAME = ".mv_metadata_studio_ui.json"
PREFERENCES_DIR = Path("/app/runtime")
IGNORED_QUERY_VALUES = {"none", "null", "undefined", "nan", "留空自动", "自动"}
SEARCH_VARIANT_KEYWORDS = (
    "performance video",
    "special film",
    "dance practice",
    "practice video",
    "mv teaser",
    "teaser",
    "highlight medley",
    "behind the scenes",
    "behind the scene",
    "making film",
    "making video",
)
YOUTUBE_STRONG_NEGATIVE_KEYWORDS = (
    "reaction",
    "reacts",
    "karaoke",
    "cover",
    "lyric video",
    "lyrics",
    "lyric",
    "fanmade",
    "mashup",
    "remix",
)
YOUTUBE_MEDIUM_NEGATIVE_KEYWORDS = (
    "official audio",
    "audio",
    "behind the scenes",
    "behind",
    "dance practice",
    "performance ver",
    "performance version",
    "live clip",
    "music bank",
    "glitterday",
    "first take",
    "one cam",
    "band live",
    "it s live",
    "making",
    "mmsub",
    "simulated vr",
    "teaser",
    "highlight medley",
    "performance video",
    "special film",
)
NON_VIDEO_SOURCE_KEYWORDS = (
    "hi res",
    "flac",
    "instrumental",
    "karaoke",
    "concert",
    "live tour",
    "bdiso",
    "bd iso",
)

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)


@app.after_request
def apply_cache_headers(response: Any) -> Any:
    path = request.path or ""
    if path == "/" or path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@dataclass
class JobOptions:
    target: str
    recursive: bool = True
    default_artist: Optional[str] = None
    poster_style: str = "basename"
    overwrite: bool = False
    timeout: int = 20
    dry_run: bool = False
    verbose: bool = False
    proxy: Optional[str] = None
    ai_provider: str = DEFAULT_AI_PROVIDER
    ai_api_key: Optional[str] = None
    ai_model: str = DEFAULT_AI_MODEL
    ai_base_url: Optional[str] = None


class JobLogHandler(logging.Handler):
    def __init__(self, state: "InMemoryJobState") -> None:
        super().__init__()
        self.state = state

    def emit(self, record: logging.LogRecord) -> None:
        self.state.append_log(self.format(record))


class InMemoryJobState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._job_id: Optional[str] = None
        self._running = False
        self._started_at: Optional[str] = None
        self._finished_at: Optional[str] = None
        self._options: dict[str, Any] = {}
        self._logs: list[str] = []
        self._stats = RunStats()
        self._processed_files = 0
        self._current_file: Optional[str] = None
        self._error: Optional[str] = None

    def start_job(self, options: JobOptions) -> tuple[bool, dict[str, Any]]:
        with self._lock:
            if self._running:
                return False, {"error": "A job is already running."}

            self._job_id = uuid.uuid4().hex[:12]
            self._running = True
            self._started_at = datetime.now().isoformat(timespec="seconds")
            self._finished_at = None
            options_dict = asdict(options)
            if options_dict.get("ai_api_key"):
                options_dict["ai_api_key"] = "***"
            self._options = options_dict
            self._logs = []
            self._stats = RunStats()
            self._processed_files = 0
            self._current_file = None
            self._error = None

            return True, self._snapshot_unlocked()

    def append_log(self, message: str) -> None:
        with self._lock:
            self._logs.append(message)

    def set_job_totals(self, scanned: int) -> None:
        with self._lock:
            self._stats.scanned = scanned

    def set_current_file(self, file_path: str, processed_files: int) -> None:
        with self._lock:
            self._current_file = file_path
            self._processed_files = processed_files

    def update_stats(self, stats: RunStats, processed_files: int) -> None:
        with self._lock:
            self._stats = stats
            self._processed_files = processed_files

    def finish_job(self, error: Optional[str] = None) -> None:
        with self._lock:
            self._running = False
            self._finished_at = datetime.now().isoformat(timespec="seconds")
            self._current_file = None
            self._error = error

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> dict[str, Any]:
        return {
            "job_id": self._job_id,
            "running": self._running,
            "started_at": self._started_at,
            "finished_at": self._finished_at,
            "options": self._options,
            "stats": {
                "scanned": self._stats.scanned,
                "success": self._stats.success,
                "skipped": self._stats.skipped,
                "failed": self._stats.failed,
                "processed": self._processed_files,
            },
            "current_file": self._current_file,
            "error": self._error,
            "log_count": len(self._logs),
        }

    def read_logs(self, cursor: int) -> dict[str, Any]:
        with self._lock:
            safe_cursor = max(0, cursor)
            lines = self._logs[safe_cursor:]
            return {
                "job_id": self._job_id,
                "running": self._running,
                "cursor": safe_cursor,
                "next_cursor": len(self._logs),
                "lines": lines,
            }


STATE = InMemoryJobState()


def resolve_preferences_path() -> Path:
    preferred_dir = PREFERENCES_DIR if PREFERENCES_DIR.exists() else BASE_DIR
    return preferred_dir / PREFERENCES_BASENAME


def load_preferences() -> dict[str, Any]:
    path = resolve_preferences_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}

    target = str(payload.get("target") or "").strip() or DEFAULT_TARGET
    return {"target": target}


def save_preferences(preferences: dict[str, Any]) -> dict[str, Any]:
    current = load_preferences()
    current.update(preferences)
    current["target"] = str(current.get("target") or "").strip() or DEFAULT_TARGET

    path = resolve_preferences_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(current, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    return current


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def parse_timeout(value: Any, default: int = 20) -> int:
    try:
        timeout = int(default if value is None else value)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout must be an integer") from exc

    if timeout < 5 or timeout > 120:
        raise ValueError("timeout must be between 5 and 120 seconds")
    return timeout


def parse_poster_style(value: Any) -> str:
    poster_style = str(value or "basename").strip().lower() or "basename"
    if poster_style not in {"basename", "folder"}:
        raise ValueError("poster_style must be 'basename' or 'folder'")
    return poster_style


def normalize_proxy_url(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None

    normalized = raw if "://" in raw else f"http://{raw}"
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("proxy must be a valid URL, e.g. http://127.0.0.1:7890")
    return normalized


def parse_job_options(payload: dict[str, Any]) -> JobOptions:
    target = str(payload.get("target", "")).strip()
    if not target:
        raise ValueError("target is required")

    default_artist_value = str(payload.get("default_artist", "")).strip()
    default_artist = default_artist_value if default_artist_value else None
    ai_provider_raw = payload.get("ai_provider", payload.get("openai_provider", DEFAULT_AI_PROVIDER))
    ai_provider = normalize_ai_provider(ai_provider_raw)
    ai_api_key_raw = payload.get("ai_api_key", payload.get("openai_api_key"))
    ai_model_raw = payload.get("ai_model", payload.get("openai_model"))
    ai_base_url_raw = payload.get("ai_base_url", payload.get("openai_base_url"))

    return JobOptions(
        target=target,
        recursive=parse_bool(payload.get("recursive"), default=True),
        default_artist=default_artist,
        poster_style=parse_poster_style(payload.get("poster_style")),
        overwrite=parse_bool(payload.get("overwrite"), default=False),
        timeout=parse_timeout(payload.get("timeout"), default=20),
        dry_run=parse_bool(payload.get("dry_run"), default=False),
        verbose=parse_bool(payload.get("verbose"), default=False),
        proxy=normalize_proxy_url(payload.get("proxy")),
        ai_provider=ai_provider,
        ai_api_key=resolve_ai_api_key(ai_provider, ai_api_key_raw),
        ai_model=resolve_ai_model(ai_provider, ai_model_raw),
        ai_base_url=resolve_ai_base_url(ai_provider, ai_base_url_raw),
    )


def validate_video_path(path_text: Any) -> Path:
    text = str(path_text or "").strip()
    if not text:
        raise ValueError("video_path is required")

    video_path = Path(text).expanduser().resolve()
    if not video_path.exists() or not video_path.is_file():
        raise ValueError("video_path does not exist")
    if video_path.suffix.lower() not in VIDEO_EXTENSIONS:
        raise ValueError("video_path is not a supported video file")
    return video_path


def build_file_item(video_file: Path, default_artist: Optional[str], poster_style: str) -> dict[str, Any]:
    parsed = infer_track_from_path(video_file, default_artist)
    poster_path = resolve_poster_path(video_file, poster_style)
    nfo_path = video_file.with_suffix(".nfo")

    return {
        "video_path": str(video_file),
        "file_name": video_file.name,
        "artist": parsed.artist if parsed else None,
        "title": parsed.title if parsed else None,
        "parsed": bool(parsed),
        "poster_path": str(poster_path),
        "poster_exists": poster_path.exists(),
        "nfo_path": str(nfo_path),
        "nfo_exists": nfo_path.exists(),
    }


def normalize_match_text(text: Optional[str]) -> str:
    value = remove_noise_fragments(str(text or ""))
    value = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", value).strip().lower()
    return re.sub(r"\s+", " ", value)


def normalize_keyword_text(text: Optional[str]) -> str:
    value = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", str(text or "")).strip().lower()
    return re.sub(r"\s+", " ", value)


def normalize_optional_query(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if normalize_keyword_text(text) in IGNORED_QUERY_VALUES:
        return None
    return text


def normalize_title_variants(target_title: Any) -> list[str]:
    raw_values = [target_title] if isinstance(target_title, str) else list(target_title or [])
    values: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = normalize_keyword_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        values.append(value)
    return values


def merge_title_keywords(target_title: Any) -> str:
    return " ".join(normalize_keyword_text(value) for value in normalize_title_variants(target_title))


def tokenize_match_text(text: Optional[str]) -> set[str]:
    normalized = normalize_match_text(text)
    if not normalized:
        return set()
    return {token for token in normalized.split(" ") if token}


def overlap_ratio(expected: set[str], actual: set[str]) -> float:
    if not expected:
        return 0.0
    return len(expected & actual) / len(expected)


def simplify_search_title(text: Optional[str]) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""

    def replace_bracket(match: re.Match[str]) -> str:
        body = normalize_match_text(match.group(0)[1:-1])
        if any(keyword in body for keyword in SEARCH_VARIANT_KEYWORDS):
            return " "
        return match.group(0)

    cleaned = re.sub(r"\[[^\]]+\]|\([^\)]+\)", replace_bracket, raw)
    for keyword in SEARCH_VARIANT_KEYWORDS:
        cleaned = re.sub(rf"(?:\s*[-–—|_/]+\s*)?{re.escape(keyword)}", " ", cleaned, flags=re.IGNORECASE)

    cleaned = remove_noise_fragments(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip("-_| ")
    return cleaned.strip()


def score_track_candidate(item: dict[str, Any], target_title: str, target_artist: str) -> int:
    title_expected = normalize_match_text(target_title)
    title_actual = normalize_match_text(item.get("track_name"))
    artist_expected = normalize_match_text(target_artist)
    artist_actual = normalize_match_text(item.get("artist_name"))
    title_expected_tokens = tokenize_match_text(title_expected)
    title_actual_tokens = tokenize_match_text(title_actual)

    score = 0
    if title_expected:
        if title_actual == title_expected:
            score += 120
        elif title_expected in title_actual:
            score += 90
        elif title_actual and title_actual in title_expected:
            score += int(60 * overlap_ratio(title_expected_tokens, title_actual_tokens))
        else:
            title_overlap = overlap_ratio(title_expected_tokens, title_actual_tokens)
            score += int(60 * title_overlap)
            if title_overlap == 0:
                score -= 120

    if artist_expected:
        if artist_actual == artist_expected:
            score += 50
        elif artist_expected in artist_actual or artist_actual in artist_expected:
            score += 35
        else:
            artist_overlap = overlap_ratio(tokenize_match_text(artist_expected), tokenize_match_text(artist_actual))
            score += int(20 * artist_overlap)
            if artist_overlap == 0:
                score -= 30

    return score


def score_track_candidate_variants(item: dict[str, Any], target_title: Any, target_artist: str) -> int:
    variants = normalize_title_variants(target_title) or [""]
    return max(score_track_candidate(item, target_title=variant, target_artist=target_artist) for variant in variants)


def resolve_candidate_identity(item: dict[str, Any]) -> dict[str, Any]:
    title = str(item.get("track_name") or "").strip()
    artist = str(item.get("artist_name") or "").strip()
    if title and not artist:
        parsed = parse_artist_title(title)
        if parsed:
            title = parsed.title
            artist = parsed.artist
    return {
        **item,
        "track_name": title or item.get("track_name"),
        "artist_name": artist or item.get("artist_name"),
    }


def score_source_candidate(item: dict[str, Any], target_title: Any, target_artist: str, source: str) -> int:
    candidate = resolve_candidate_identity(item)
    score = score_track_candidate_variants(candidate, target_title=target_title, target_artist=target_artist)
    text_blob = normalize_keyword_text(
        " ".join(str(candidate.get(key) or "") for key in ("artist_name", "track_name", "collection_name"))
    )

    if source == "lgych.com":
        if "bugs mp4" in text_blob or " 4k" in f" {text_blob}" or "2160p" in text_blob:
            score += 20
        if any(keyword in text_blob for keyword in NON_VIDEO_SOURCE_KEYWORDS):
            score -= 160
    elif source == "Bugs":
        if str(candidate.get("mv_id") or "").strip() not in {"", "0"}:
            score += 30

    return score


def score_youtube_candidate(item: dict[str, Any], target_title: Any, target_artist: str) -> int:
    title_variants = normalize_title_variants(target_title)
    primary_title = normalize_keyword_text(title_variants[0]) if title_variants else ""
    secondary_title = normalize_keyword_text(title_variants[-1]) if title_variants else ""
    score = score_track_candidate_variants(
        {
            "track_name": item.get("title"),
            "artist_name": item.get("channel"),
        },
        target_title=title_variants,
        target_artist=target_artist,
    )

    expected_tokens = tokenize_match_text(f"{target_artist} {' '.join(title_variants)}")
    actual_tokens = tokenize_match_text(f"{item.get('title') or ''} {item.get('channel') or ''}")
    score += int(30 * overlap_ratio(expected_tokens, actual_tokens))

    normalized_title = normalize_match_text(item.get("title"))
    normalized_target_artist = normalize_match_text(target_artist)
    keyword_title = normalize_keyword_text(item.get("title"))
    keyword_target_title = merge_title_keywords(title_variants)

    if primary_title and primary_title != secondary_title and primary_title in keyword_title:
        score += 45
    if normalized_target_artist and normalized_target_artist in normalized_title:
        score += 8
    if "official" in keyword_title:
        score += 12
    if "music video" in keyword_title or re.search(r"\bmv\b", keyword_title):
        score += 18

    for keyword in YOUTUBE_STRONG_NEGATIVE_KEYWORDS:
        if keyword in keyword_title and keyword not in keyword_target_title:
            score -= 70

    for keyword in YOUTUBE_MEDIUM_NEGATIVE_KEYWORDS:
        if keyword in keyword_title and keyword not in keyword_target_title:
            score -= 65

    return score


def youtube_source_label(item: dict[str, Any]) -> str:
    channel = normalize_keyword_text(item.get("channel"))
    return "VEVO" if "vevo" in channel else "YouTube"


def youtube_source_priority(item: dict[str, Any], target_title: Any) -> int:
    title_variants = normalize_title_variants(target_title)
    keyword_title = normalize_keyword_text(item.get("title"))
    keyword_target_title = merge_title_keywords(title_variants)
    primary_title = normalize_keyword_text(title_variants[0]) if title_variants else ""
    secondary_title = normalize_keyword_text(title_variants[-1]) if title_variants else ""
    source = youtube_source_label(item)
    primary_markers = ("official video", "official mv", "music video")
    has_primary_marker = any(marker in keyword_title for marker in primary_markers) or bool(re.search(r"\bmv\b", keyword_title))
    has_unwanted_variant = any(keyword in keyword_title and keyword not in keyword_target_title for keyword in YOUTUBE_MEDIUM_NEGATIVE_KEYWORDS)
    has_precise_title_match = bool(primary_title and primary_title != secondary_title and primary_title in keyword_title)

    if source == "VEVO" and not has_unwanted_variant:
        return 760
    if has_precise_title_match and not has_unwanted_variant:
        return 735
    if has_primary_marker and not has_unwanted_variant:
        return 720
    if source == "VEVO":
        return 680
    if has_precise_title_match:
        return 640
    return 520


def prioritize_youtube_candidates(
    items: list[dict[str, Any]], target_title: Any, target_artist: str, max_results: int = 6
) -> list[dict[str, Any]]:
    if not items:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in items:
        if not item.get("thumbnail"):
            continue
        score = score_youtube_candidate(item, target_title=target_title, target_artist=target_artist)
        scored.append((score, item))

    if not scored:
        return []

    scored.sort(
        key=lambda pair: (
            pair[0],
            youtube_source_priority(pair[1], target_title),
            int(bool(pair[1].get("view_count"))),
            len(normalize_match_text(pair[1].get("title"))),
        ),
        reverse=True,
    )

    if merge_title_keywords(target_title) or normalize_match_text(target_artist):
        filtered = [pair for pair in scored if pair[0] >= 20]
        if not filtered and scored[0][0] >= 0:
            filtered = [pair for pair in scored if pair[0] >= 0]
        if not filtered:
            filtered = scored[:1]
    else:
        filtered = scored

    return [item for _, item in filtered[: max(1, min(max_results, 10))]]


def prioritize_source_candidates(
    items: list[dict[str, Any]], target_title: Any, target_artist: str, source: str, max_results: int = 5
) -> list[dict[str, Any]]:
    if not items:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in items:
        if not item.get("artwork_url"):
            continue
        score = score_source_candidate(item, target_title=target_title, target_artist=target_artist, source=source)
        scored.append((score, item))

    if not scored:
        return []

    scored.sort(
        key=lambda pair: (
            pair[0],
            len(normalize_match_text(pair[1].get("track_name"))),
            len(normalize_match_text(pair[1].get("artist_name"))),
        ),
        reverse=True,
    )
    best_score = scored[0][0]
    threshold = 30 if best_score >= 100 else 0
    filtered = [pair for pair in scored if pair[0] >= threshold]
    if not filtered:
        filtered = scored[:1]
    return [item for _, item in filtered[: max(1, min(max_results, 10))]]


def dedupe_and_sort_candidates(candidates: list[dict[str, Any]], max_results: int = 16) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for item in candidates:
        image_url = str(item.get("image_url") or "").strip()
        if not image_url:
            continue
        key = image_url.lower()
        current = deduped.get(key)
        if current is None or int(item.get("_sort_score") or 0) > int(current.get("_sort_score") or 0):
            deduped[key] = item

    ranked = sorted(
        deduped.values(),
        key=lambda item: (
            int(item.get("_sort_score") or 0),
            len(str(item.get("title") or "")),
        ),
        reverse=True,
    )

    result: list[dict[str, Any]] = []
    for item in ranked[: max(1, min(max_results, 24))]:
        clean_item = dict(item)
        clean_item.pop("_sort_score", None)
        result.append(clean_item)
    return result


def prioritize_itunes_candidates(
    items: list[dict[str, Any]], target_title: str, target_artist: str, max_results: int = 6
) -> list[dict[str, Any]]:
    if not items:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for item in items:
        if not item.get("artwork_url"):
            continue
        score = score_track_candidate(item, target_title=target_title, target_artist=target_artist)
        scored.append((score, item))

    if not scored:
        return []

    scored.sort(
        key=lambda pair: (
            pair[0],
            len(normalize_match_text(pair[1].get("track_name"))),
            len(normalize_match_text(pair[1].get("artist_name"))),
        ),
        reverse=True,
    )
    best_score = scored[0][0]
    if best_score >= 100:
        filtered = [pair for pair in scored if pair[0] >= 50]
    else:
        filtered = [pair for pair in scored if pair[0] > -60]

    return [item for _, item in filtered[: max(1, min(max_results, 20))]]


def build_poster_candidates(
    video_path: Path,
    default_artist: Optional[str],
    query: Optional[str],
    timeout: int,
    proxy_url: Optional[str],
) -> dict[str, Any]:
    parsed = infer_track_from_path(video_path, default_artist)
    custom_query = bool(query and query.strip())

    video_candidates: list[dict[str, Any]] = []
    fallback_candidates: list[dict[str, Any]] = []

    if custom_query:
        search_artist = ""
        search_title = query.strip() if query else ""
        base_query = search_title
        target_title_variants = normalize_title_variants([search_title])
    elif parsed:
        search_artist = parsed.artist
        search_title = simplify_search_title(parsed.title) or parsed.title
        base_query = " ".join(part for part in (search_artist, search_title) if part).strip()
        target_title_variants = normalize_title_variants([parsed.title, search_title])
    else:
        search_artist = ""
        search_title = simplify_search_title(video_path.stem) or video_path.stem
        base_query = search_title
        target_title_variants = normalize_title_variants([video_path.stem, search_title])

    if not target_title_variants:
        target_title_variants = normalize_title_variants([search_title, base_query])

    try:
        youtube_raw = search_youtube_candidates(
            f"{base_query} official music video",
            proxy_url=proxy_url,
            max_results=10,
        )
    except Exception:  # pragma: no cover - network edge
        youtube_raw = []
    youtube_refined = prioritize_youtube_candidates(
        youtube_raw,
        target_title=target_title_variants,
        target_artist=search_artist,
        max_results=5,
    )
    for idx, item in enumerate(youtube_refined):
        image_url = item.get("thumbnail")
        if not image_url:
            continue
        source_label = youtube_source_label(item)
        score = score_youtube_candidate(item, target_title=target_title_variants, target_artist=search_artist)
        video_candidates.append(
            {
                "id": f"yt-{idx}",
                "source": source_label,
                "title": item.get("title") or "YouTube Thumbnail",
                "subtitle": item.get("channel") or "YouTube",
                "image_url": image_url,
                "webpage_url": item.get("webpage_url"),
                "_sort_score": youtube_source_priority(item, target_title_variants) + score,
            }
        )

    try:
        lgych_raw = search_lgych_candidates(
            artist=search_artist,
            title=search_title,
            timeout=timeout,
            proxy_url=proxy_url,
            limit=10,
        )
    except Exception:  # pragma: no cover - network edge
        lgych_raw = []
    lgych_refined = prioritize_source_candidates(
        lgych_raw,
        target_title=target_title_variants,
        target_artist=search_artist,
        source="lgych.com",
        max_results=4,
    )
    for idx, item in enumerate(lgych_refined):
        image_url = item.get("artwork_url")
        if not image_url:
            continue
        title = item.get("track_name")
        video_candidates.append(
            {
                "id": f"lgych-{idx}",
                "source": "lgych.com",
                "title": title or "lgych Poster",
                "subtitle": item.get("collection_name") or "lgych.com",
                "image_url": image_url,
                "_sort_score": 640
                + score_source_candidate(item, target_title=target_title_variants, target_artist=search_artist, source="lgych.com"),
            }
        )

    try:
        bugs_raw = search_bugs_candidates(
            artist=search_artist,
            title=search_title,
            timeout=timeout,
            proxy_url=proxy_url,
            limit=8,
        )
    except Exception:  # pragma: no cover - network edge
        bugs_raw = []
    bugs_refined = prioritize_source_candidates(
        bugs_raw,
        target_title=target_title_variants,
        target_artist=search_artist,
        source="Bugs",
        max_results=4,
    )
    for idx, item in enumerate(bugs_refined):
        image_url = item.get("artwork_url")
        if not image_url:
            continue

        title_parts = [item.get("track_name"), item.get("artist_name")]
        title = " - ".join(part for part in title_parts if part)
        video_candidates.append(
            {
                "id": f"bugs-{idx}",
                "source": "Bugs",
                "title": title or "Bugs MV",
                "subtitle": item.get("collection_name") or "Bugs",
                "image_url": image_url,
                "webpage_url": item.get("webpage_url"),
                "_sort_score": 600
                + score_source_candidate(item, target_title=target_title_variants, target_artist=search_artist, source="Bugs"),
            }
        )

    deduped_video = dedupe_and_sort_candidates(video_candidates, max_results=12)

    if deduped_video:
        return {
            "query": base_query,
            "parsed": {
                "artist": parsed.artist,
                "title": parsed.title,
            }
            if parsed
            else None,
            "candidates": deduped_video,
        }

    try:
        itunes_raw = search_itunes_candidates(
            artist=search_artist,
            title=search_title,
            timeout=timeout,
            proxy_url=proxy_url,
            limit=12,
        )
    except Exception:  # pragma: no cover - network edge
        itunes_raw = []
    itunes_refined = prioritize_itunes_candidates(
        itunes_raw,
        target_title=target_title_variants[0] if target_title_variants else search_title,
        target_artist=search_artist,
        max_results=4,
    )
    for idx, item in enumerate(itunes_refined):
        image_url = item.get("artwork_url")
        if not image_url:
            continue

        title_parts = [item.get("track_name"), item.get("artist_name")]
        title = " - ".join(part for part in title_parts if part)
        subtitle = item.get("collection_name") or "iTunes"
        fallback_candidates.append(
            {
                "id": f"itunes-{idx}",
                "source": "iTunes",
                "title": title or "iTunes Artwork",
                "subtitle": subtitle,
                "image_url": image_url,
                "_sort_score": 240 + score_track_candidate_variants(item, target_title_variants, search_artist),
            }
        )

    try:
        deezer_raw = search_deezer_candidates(
            artist=search_artist,
            title=search_title,
            timeout=timeout,
            proxy_url=proxy_url,
            limit=6,
        )
    except Exception:  # pragma: no cover - network edge
        deezer_raw = []
    deezer_refined = prioritize_source_candidates(
        deezer_raw,
        target_title=target_title_variants,
        target_artist=search_artist,
        source="Deezer",
        max_results=3,
    )
    for idx, item in enumerate(deezer_refined):
        image_url = item.get("artwork_url")
        if not image_url:
            continue
        title_parts = [item.get("track_name"), item.get("artist_name")]
        title = " - ".join(part for part in title_parts if part)
        fallback_candidates.append(
            {
                "id": f"deezer-{idx}",
                "source": "Deezer",
                "title": title or "Deezer Artwork",
                "subtitle": item.get("collection_name") or "Deezer",
                "image_url": image_url,
                "_sort_score": 220 + score_source_candidate(item, target_title_variants, search_artist, "Deezer"),
            }
        )

    try:
        audiodb_raw = search_audiodb_candidates(
            artist=search_artist,
            title=search_title,
            timeout=timeout,
            proxy_url=proxy_url,
            limit=6,
        )
    except Exception:  # pragma: no cover - network edge
        audiodb_raw = []
    audiodb_refined = prioritize_source_candidates(
        audiodb_raw,
        target_title=target_title_variants,
        target_artist=search_artist,
        source="AudioDB",
        max_results=3,
    )
    for idx, item in enumerate(audiodb_refined):
        image_url = item.get("artwork_url")
        if not image_url:
            continue
        title_parts = [item.get("track_name"), item.get("artist_name")]
        title = " - ".join(part for part in title_parts if part)
        fallback_candidates.append(
            {
                "id": f"audiodb-{idx}",
                "source": "AudioDB",
                "title": title or "AudioDB Artwork",
                "subtitle": item.get("collection_name") or "AudioDB",
                "image_url": image_url,
                "_sort_score": 200 + score_source_candidate(item, target_title_variants, search_artist, "AudioDB"),
            }
        )

    return {
        "query": base_query,
        "parsed": {
            "artist": parsed.artist,
            "title": parsed.title,
        }
        if parsed
        else None,
        "candidates": dedupe_and_sort_candidates(fallback_candidates, max_results=10),
    }


def check_proxy_latency(proxy_url: str, timeout: int) -> dict[str, Any]:
    headers = {"User-Agent": "mv-emby-scraper/0.1"}
    proxies = {"http": proxy_url, "https": proxy_url}
    targets = [
        ("iTunes", "https://itunes.apple.com/search?term=test&entity=song&limit=1"),
        ("YouTube", "https://www.youtube.com/generate_204"),
        ("lgych", "https://www.lgych.com/"),
    ]

    checks: list[dict[str, Any]] = []
    latencies: list[float] = []

    for name, url in targets:
        started = perf_counter()
        try:
            response = requests.get(url, headers=headers, timeout=timeout, proxies=proxies)
            elapsed_ms = round((perf_counter() - started) * 1000, 1)
            ok = response.status_code < 400
            if ok:
                latencies.append(elapsed_ms)
            checks.append(
                {
                    "name": name,
                    "ok": ok,
                    "latency_ms": elapsed_ms,
                    "status_code": response.status_code,
                }
            )
        except requests.RequestException as exc:
            elapsed_ms = round((perf_counter() - started) * 1000, 1)
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "latency_ms": elapsed_ms,
                    "error": str(exc),
                }
            )

    overall_ok = all(check["ok"] for check in checks) if checks else False
    average_latency = round(sum(latencies) / len(latencies), 1) if latencies else None

    return {
        "ok": overall_ok,
        "latency_ms": average_latency,
        "checks": checks,
    }


def ensure_nfo_after_manual_apply(
    video_path: Path,
    poster_style: str,
    timeout: int,
    proxy_url: Optional[str],
    default_artist: Optional[str],
    ai_provider: str = DEFAULT_AI_PROVIDER,
    ai_api_key: Optional[str] = None,
    ai_model: str = DEFAULT_AI_MODEL,
    ai_base_url: Optional[str] = None,
) -> dict[str, Any]:
    nfo_path = video_path.with_suffix(".nfo")
    if nfo_path.exists():
        return {
            "nfo_path": str(nfo_path),
            "nfo_exists": True,
            "nfo_status": "existing",
        }

    args = SimpleNamespace(
        default_artist=default_artist,
        poster_style=poster_style,
        overwrite=False,
        timeout=timeout,
        proxy=proxy_url,
        ai_provider=ai_provider,
        ai_api_key=ai_api_key,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
    )

    status = process_video(video_path, args)
    if nfo_path.exists():
        return {
            "nfo_path": str(nfo_path),
            "nfo_exists": True,
            "nfo_status": status,
        }

    parsed = infer_track_from_path(video_path, default_artist)
    artist = parsed.artist if parsed else (default_artist or "Unknown Artist")
    title = parsed.title if parsed else video_path.stem

    metadata = CombinedMetadata(artist=artist, title=title)
    poster_path = resolve_poster_path(video_path, poster_style)
    poster_file_name = poster_path.name if poster_path.exists() else None
    write_nfo(metadata, nfo_path, poster_file_name)

    return {
        "nfo_path": str(nfo_path),
        "nfo_exists": nfo_path.exists(),
        "nfo_status": "minimal",
    }


def sync_nfo_thumb(video_path: Path, poster_path: Path) -> bool:
    nfo_path = video_path.with_suffix(".nfo")
    if not nfo_path.exists():
        return False

    try:
        tree = ET.parse(nfo_path)
        root = tree.getroot()
    except ET.ParseError:
        return False

    thumb_element = root.find("thumb")
    if thumb_element is None:
        thumb_element = ET.SubElement(root, "thumb")
    thumb_element.text = poster_path.name

    title_element = root.find("title")
    if title_element is not None and title_element.text:
        sanitized_title = remove_noise_fragments(title_element.text)
        if sanitized_title:
            title_element.text = sanitized_title

    tree.write(nfo_path, encoding="utf-8", xml_declaration=True)
    return True


def run_job(job_id: str, options: JobOptions) -> None:
    root_logger = logging.getLogger()
    old_level = root_logger.level

    handler = JobLogHandler(STATE)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S"))

    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG if options.verbose else logging.INFO)

    args = SimpleNamespace(
        default_artist=options.default_artist,
        poster_style=options.poster_style,
        overwrite=options.overwrite,
        timeout=options.timeout,
        proxy=options.proxy,
        ai_provider=options.ai_provider,
        ai_api_key=options.ai_api_key,
        ai_model=options.ai_model,
        ai_base_url=options.ai_base_url,
    )

    fatal_error: Optional[str] = None

    try:
        target = Path(options.target).expanduser().resolve()
        if not target.exists():
            logging.error("Target path does not exist: %s", target)
            fatal_error = "target path does not exist"
            return

        files = list(collect_video_files(target, options.recursive))
        if not files:
            logging.error("No video files found at target: %s", target)
            fatal_error = "no video files found"
            return

        stats = RunStats(scanned=len(files))
        STATE.set_job_totals(scanned=stats.scanned)
        logging.info("Job started. target=%s total_files=%s", target, stats.scanned)

        for idx, video_file in enumerate(files, start=1):
            STATE.set_current_file(str(video_file), processed_files=idx - 1)
            logging.info("[%s/%s] Processing %s", idx, stats.scanned, video_file.name)

            if options.dry_run:
                parsed = infer_track_from_path(video_file, options.default_artist)
                if parsed:
                    logging.info("[DRY-RUN] %s => %s - %s", video_file.name, parsed.artist, parsed.title)
                    stats.success += 1
                else:
                    logging.warning("[DRY-RUN] Failed to parse: %s", video_file)
                    stats.failed += 1
                STATE.update_stats(stats, processed_files=idx)
                continue

            status = process_video(video_file, args)
            if status == "success":
                stats.success += 1
            elif status == "skipped":
                stats.skipped += 1
            else:
                stats.failed += 1

            STATE.update_stats(stats, processed_files=idx)

        logging.info(
            "Done. scanned=%s success=%s skipped=%s failed=%s",
            stats.scanned,
            stats.success,
            stats.skipped,
            stats.failed,
        )
    except Exception as exc:  # pragma: no cover - defensive guard
        logging.exception("Unexpected crash while running job")
        fatal_error = str(exc)
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(old_level)
        STATE.finish_job(error=fatal_error)


@app.get("/")
def index() -> str:
    preferences = load_preferences()
    static_candidates = (
        BASE_DIR / "static" / "styles.css",
        BASE_DIR / "static" / "app.js",
    )
    version_parts: list[str] = []
    for candidate in static_candidates:
        try:
            stat = candidate.stat()
            version_parts.append(f"{candidate.name}:{stat.st_size}:{stat.st_mtime_ns}")
        except OSError:
            continue

    static_version = hashlib.sha1("|".join(version_parts).encode("utf-8")).hexdigest()[:12] if version_parts else "0"

    return render_template(
        "index.html",
        default_target=preferences.get("target") or DEFAULT_TARGET,
        default_ai_provider=DEFAULT_AI_PROVIDER,
        default_ai_model=DEFAULT_AI_MODEL,
        static_version=static_version,
    )


@app.get("/api/preferences")
def api_preferences() -> Any:
    return jsonify(load_preferences())


@app.post("/api/preferences")
def api_save_preferences() -> Any:
    payload = request.get_json(silent=True) or {}
    target = str(payload.get("target", "")).strip() or DEFAULT_TARGET

    try:
        target_path = Path(target).expanduser().resolve()
    except OSError as exc:
        return jsonify({"error": f"invalid target path: {exc}"}), 400

    saved = save_preferences({"target": str(target_path)})
    return jsonify(saved)


@app.get("/api/status")
def api_status() -> Any:
    return jsonify(STATE.snapshot())


@app.get("/api/logs")
def api_logs() -> Any:
    cursor_text = request.args.get("cursor", "0")
    try:
        cursor = int(cursor_text)
    except ValueError:
        cursor = 0
    return jsonify(STATE.read_logs(cursor))


@app.get("/api/files")
def api_files() -> Any:
    target_text = str(request.args.get("target", "")).strip()

    recursive = parse_bool(request.args.get("recursive"), default=True)
    default_artist_raw = str(request.args.get("default_artist", "")).strip()
    default_artist = default_artist_raw if default_artist_raw else None

    try:
        poster_style = parse_poster_style(request.args.get("poster_style"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    target = Path(target_text).expanduser().resolve()
    if not target.exists():
        return jsonify({"error": f"target path does not exist: {target}"}), 400

    files = list(collect_video_files(target, recursive))
    payload_files = [build_file_item(video, default_artist, poster_style) for video in files]

    return jsonify(
        {
            "target": str(target),
            "recursive": recursive,
            "count": len(payload_files),
            "files": payload_files,
        }
    )


@app.post("/api/poster/search")
def api_poster_search() -> Any:
    payload = request.get_json(silent=True) or {}

    try:
        video_path = validate_video_path(payload.get("video_path"))
        timeout = parse_timeout(payload.get("timeout"), default=20)
        proxy_url = normalize_proxy_url(payload.get("proxy"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    default_artist_raw = str(payload.get("default_artist", "")).strip()
    default_artist = default_artist_raw if default_artist_raw else None

    query = normalize_optional_query(payload.get("query"))

    try:
        result = build_poster_candidates(
            video_path=video_path,
            default_artist=default_artist,
            query=query,
            timeout=timeout,
            proxy_url=proxy_url,
        )
    except Exception as exc:
        return jsonify({"error": f"search failed: {exc}"}), 500

    return jsonify(
        {
            "video_path": str(video_path),
            "parsed": result.get("parsed"),
            "query": result.get("query"),
            "count": len(result.get("candidates", [])),
            "candidates": result.get("candidates", []),
        }
    )


@app.post("/api/poster/apply")
def api_poster_apply() -> Any:
    payload = request.get_json(silent=True) or {}

    image_url = str(payload.get("image_url", "")).strip()
    if not image_url or not image_url.lower().startswith(("http://", "https://")):
        return jsonify({"error": "image_url must be a valid http/https URL"}), 400

    try:
        video_path = validate_video_path(payload.get("video_path"))
        poster_style = parse_poster_style(payload.get("poster_style"))
        timeout = parse_timeout(payload.get("timeout"), default=20)
        proxy_url = normalize_proxy_url(payload.get("proxy"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    poster_path = resolve_poster_path(video_path, poster_style)
    default_artist_raw = str(payload.get("default_artist", "")).strip()
    default_artist = default_artist_raw if default_artist_raw else None
    ai_provider = normalize_ai_provider(payload.get("ai_provider", payload.get("openai_provider", DEFAULT_AI_PROVIDER)))
    ai_api_key = resolve_ai_api_key(ai_provider, payload.get("ai_api_key", payload.get("openai_api_key")))
    ai_model = resolve_ai_model(ai_provider, payload.get("ai_model", payload.get("openai_model")))
    ai_base_url = resolve_ai_base_url(ai_provider, payload.get("ai_base_url", payload.get("openai_base_url")))

    try:
        success = download_poster(image_url, poster_path, timeout=timeout, proxy_url=proxy_url)
    except Exception as exc:
        return jsonify({"error": f"poster download failed: {exc}"}), 500

    if not success:
        return jsonify({"error": "poster format is not supported"}), 422

    nfo_result = ensure_nfo_after_manual_apply(
        video_path=video_path,
        poster_style=poster_style,
        timeout=timeout,
        proxy_url=proxy_url,
        default_artist=default_artist,
        ai_provider=ai_provider,
        ai_api_key=ai_api_key,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
    )
    nfo_synced = sync_nfo_thumb(video_path, poster_path)

    return jsonify(
        {
            "video_path": str(video_path),
            "poster_path": str(poster_path),
            "poster_exists": poster_path.exists(),
            "nfo_synced": nfo_synced,
            "nfo_path": nfo_result["nfo_path"],
            "nfo_exists": nfo_result["nfo_exists"],
            "nfo_status": nfo_result["nfo_status"],
        }
    )


@app.post("/api/proxy/check")
def api_proxy_check() -> Any:
    payload = request.get_json(silent=True) or {}
    raw_proxy = str(payload.get("proxy", "")).strip()
    if not raw_proxy:
        return jsonify({"error": "proxy is required"}), 400

    try:
        proxy_url = normalize_proxy_url(raw_proxy)
        timeout = parse_timeout(payload.get("timeout"), default=8)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    result = check_proxy_latency(proxy_url=proxy_url, timeout=timeout)
    return jsonify({"proxy": proxy_url, **result})


@app.post("/api/start")
def api_start() -> Any:
    payload = request.get_json(silent=True) or {}
    try:
        options = parse_job_options(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    ok, snapshot = STATE.start_job(options)
    if not ok:
        return jsonify(snapshot), 409

    job_id = snapshot["job_id"]
    thread = threading.Thread(target=run_job, args=(job_id, options), daemon=True)
    thread.start()

    return jsonify(snapshot), 202


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run MV Emby Scraper web console")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=7860, help="Port to listen")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
