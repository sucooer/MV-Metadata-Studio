package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultTarget     = "/media"
	defaultAIProvider = "openai"
	defaultAIModel    = "gpt-4.1-mini"
	preferencesFile   = ".mv_metadata_studio_ui.json"
)

var (
	videoExtensions = map[string]bool{
		".mp4": true, ".mkv": true, ".avi": true, ".mov": true, ".wmv": true,
		".flv": true, ".m4v": true, ".webm": true, ".ts": true,
	}
	noiseKeywords          = []string{"official", "music video", "mv", "lyrics", "lyric", "live", "karaoke", "4k", "1080p", "hd", "中字", "完整版", "shorts"}
	platformKeywords       = []string{"prores", "blu-ray", "bluray", "blu ray", "bugs", "master", "web-dl", "webdl", "web dl", "melon", "gomtv", "genie", "flo", "spotify", "apple music", "youtube music", "deezer", "tidal", "qq music", "netease", "netease cloud music", "line music", "linemusic", "amazon music", "joox", "网易云", "酷狗", "酷我", "咪咕"}
	ignoredQueryValues     = map[string]bool{"none": true, "null": true, "undefined": true, "nan": true, "留空自动": true, "自动": true}
	nonVideoSourceKeywords = []string{"hi res", "flac", "instrumental", "karaoke", "concert", "live tour", "bdiso", "bd iso"}
	aiProviderPresets      = map[string]struct {
		BaseURL string
		Model   string
	}{
		"openai":      {BaseURL: "https://api.openai.com/v1", Model: "gpt-4.1-mini"},
		"openrouter":  {BaseURL: "https://openrouter.ai/api/v1", Model: "openai/gpt-4.1-mini"},
		"deepseek":    {BaseURL: "https://api.deepseek.com/v1", Model: "deepseek-chat"},
		"siliconflow": {BaseURL: "https://api.siliconflow.cn/v1", Model: "Qwen/Qwen2.5-7B-Instruct"},
		"custom":      {BaseURL: "http://127.0.0.1:11434/v1", Model: "gpt-4.1-mini"},
	}
)

type parsedTrack struct {
	Artist string `json:"artist"`
	Title  string `json:"title"`
	Raw    string `json:"raw"`
}

type metadata struct {
	Artist          string
	Title           string
	Album           string
	Plot            string
	Premiered       string
	Year            string
	Studio          string
	Genre           string
	ThumbURL        string
	YoutubeURL      string
	YoutubeID       string
	DurationSeconds int
	Tagline         string
	Rating          *float64
	UserRating      *int
	Votes           *int
}

type runStats struct {
	Scanned   int `json:"scanned"`
	Processed int `json:"processed"`
	Success   int `json:"success"`
	Skipped   int `json:"skipped"`
	Failed    int `json:"failed"`
}

type jobOptions struct {
	Target      string `json:"target"`
	Recursive   bool   `json:"recursive"`
	PosterStyle string `json:"poster_style"`
	Overwrite   bool   `json:"overwrite"`
	Timeout     int    `json:"timeout"`
	DryRun      bool   `json:"dry_run"`
	Verbose     bool   `json:"verbose"`
	Proxy       string `json:"proxy"`
	AIProvider  string `json:"ai_provider"`
	AIAPIKey    string `json:"ai_api_key"`
	AIModel     string `json:"ai_model"`
	AIBaseURL   string `json:"ai_base_url"`
}

type fileItem struct {
	VideoPath    string `json:"video_path"`
	FileName     string `json:"file_name"`
	Artist       string `json:"artist,omitempty"`
	Title        string `json:"title,omitempty"`
	Parsed       bool   `json:"parsed"`
	PosterPath   string `json:"poster_path"`
	PosterExists bool   `json:"poster_exists"`
	NFOPath      string `json:"nfo_path"`
	NFOExists    bool   `json:"nfo_exists"`
}

type candidate struct {
	ID         string `json:"id"`
	Source     string `json:"source"`
	Title      string `json:"title"`
	Subtitle   string `json:"subtitle,omitempty"`
	ImageURL   string `json:"image_url"`
	WebpageURL string `json:"webpage_url,omitempty"`
	SortScore  int    `json:"-"`
}

type preferences struct {
	Target string `json:"target"`
}

type jobState struct {
	mu          sync.Mutex
	running     bool
	startedAt   string
	finishedAt  string
	currentFile string
	logs        []string
	stats       runStats
}

func (s *jobState) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("A job is already running.")
	}
	s.running = true
	s.startedAt = time.Now().Format(time.RFC3339)
	s.finishedAt = ""
	s.currentFile = ""
	s.logs = nil
	s.stats = runStats{}
	return nil
}

func (s *jobState) appendLog(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := time.Now().Format("15:04:05")
	s.logs = append(s.logs, fmt.Sprintf("%s | INFO | %s", ts, message))
}

func (s *jobState) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"running":      s.running,
		"started_at":   s.startedAt,
		"finished_at":  s.finishedAt,
		"current_file": s.currentFile,
		"stats":        s.stats,
	}
}

func (s *jobState) readLogs(cursor int) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor < 0 || cursor > len(s.logs) {
		cursor = 0
	}
	lines := append([]string{}, s.logs[cursor:]...)
	return map[string]any{"lines": lines, "next_cursor": len(s.logs)}
}

type app struct {
	rootDir      string
	templatePath string
	staticDir    string
	prefsDir     string
	state        *jobState
	client       *http.Client
}

func main() {
	host := "0.0.0.0"
	port := "7860"
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--host":
			if i+1 < len(os.Args) {
				host = os.Args[i+1]
				i++
			}
		case "--port":
			if i+1 < len(os.Args) {
				port = os.Args[i+1]
				i++
			}
		}
	}

	root, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	application := &app{
		rootDir:      root,
		templatePath: filepath.Join(root, "mv_scraper", "templates", "index.html"),
		staticDir:    filepath.Join(root, "mv_scraper", "static"),
		prefsDir:     "/app/runtime",
		state:        &jobState{},
		client:       &http.Client{Timeout: 20 * time.Second},
	}

	mux := http.NewServeMux()
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir(application.staticDir))))
	mux.HandleFunc("/", application.handleIndex)
	mux.HandleFunc("/api/preferences", application.handlePreferences)
	mux.HandleFunc("/api/status", application.handleStatus)
	mux.HandleFunc("/api/logs", application.handleLogs)
	mux.HandleFunc("/api/files", application.handleFiles)
	mux.HandleFunc("/api/poster/search", application.handlePosterSearch)
	mux.HandleFunc("/api/poster/apply", application.handlePosterApply)
	mux.HandleFunc("/api/proxy/check", application.handleProxyCheck)
	mux.HandleFunc("/api/start", application.handleStart)

	server := &http.Server{
		Addr:    net.JoinHostPort(host, port),
		Handler: noCache(mux),
	}

	log.Printf("listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
		}
		next.ServeHTTP(w, r)
	})
}

func (a *app) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	raw, err := os.ReadFile(a.templatePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	prefs := a.loadPreferences()
	staticVersion := a.staticVersion()
	htmlBody := string(raw)
	replacer := strings.NewReplacer(
		"__DEFAULT_TARGET__", html.EscapeString(prefs.Target),
		"__DEFAULT_AI_PROVIDER__", html.EscapeString(defaultAIProvider),
		"__DEFAULT_AI_MODEL__", html.EscapeString(defaultAIModel),
		"__STATIC_VERSION__", html.EscapeString(staticVersion),
	)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, replacer.Replace(htmlBody))
}

func (a *app) handlePreferences(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, a.loadPreferences())
	case http.MethodPost:
		var payload preferences
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		target := strings.TrimSpace(payload.Target)
		if target == "" {
			target = defaultTarget
		}
		abs, err := filepath.Abs(target)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid target path: %v", err))
			return
		}
		prefs := preferences{Target: abs}
		if err := a.savePreferences(prefs); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, prefs)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (a *app) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.state.snapshot())
}

func (a *app) handleLogs(w http.ResponseWriter, r *http.Request) {
	cursor, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
	writeJSON(w, http.StatusOK, a.state.readLogs(cursor))
}

func (a *app) handleFiles(w http.ResponseWriter, r *http.Request) {
	targetText := strings.TrimSpace(r.URL.Query().Get("target"))
	if targetText == "" {
		writeError(w, http.StatusBadRequest, "target path does not exist")
		return
	}
	recursive := parseBool(r.URL.Query().Get("recursive"), true)
	posterStyle, err := parsePosterStyle(r.URL.Query().Get("poster_style"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	target, err := filepath.Abs(targetText)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if _, err := os.Stat(target); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("target path does not exist: %s", target))
		return
	}
	files := collectVideoFiles(target, recursive)
	log.Printf("api/files target=%s recursive=%t found=%d", target, recursive, len(files))
	items := make([]fileItem, 0, len(files))
	for _, file := range files {
		items = append(items, buildFileItem(file, posterStyle))
	}
	writeJSON(w, http.StatusOK, map[string]any{"target": target, "recursive": recursive, "count": len(items), "files": items})
}

func (a *app) handlePosterSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	videoPath, err := validateVideoPath(payload["video_path"])
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	timeout, err := parseTimeout(payload["timeout"], 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	proxyURL, err := normalizeProxyURL(anyToString(payload["proxy"]))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	query := normalizeOptionalQuery(anyToString(payload["query"]))
	result := a.buildPosterCandidates(videoPath, query, timeout, proxyURL)
	writeJSON(w, http.StatusOK, map[string]any{
		"video_path": videoPath,
		"parsed":     result.Parsed,
		"query":      result.Query,
		"count":      len(result.Candidates),
		"candidates": result.Candidates,
	})
}

func (a *app) handlePosterApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	videoPath, err := validateVideoPath(payload["video_path"])
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	imageURL := strings.TrimSpace(anyToString(payload["image_url"]))
	if !(strings.HasPrefix(imageURL, "http://") || strings.HasPrefix(imageURL, "https://")) {
		writeError(w, http.StatusBadRequest, "image_url must be a valid http/https URL")
		return
	}
	posterStyle, err := parsePosterStyle(anyToString(payload["poster_style"]))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	timeout, err := parseTimeout(payload["timeout"], 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	proxyURL, err := normalizeProxyURL(anyToString(payload["proxy"]))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts := parseAIOptions(payload)
	posterPath := resolvePosterPath(videoPath, posterStyle)
	ok, err := downloadPoster(imageURL, posterPath, timeout, proxyURL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("poster download failed: %v", err))
		return
	}
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "poster format is not supported")
		return
	}
	nfoStatus, err := a.ensureNFOAfterManualApply(videoPath, posterStyle, timeout, proxyURL, opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	synced := syncNFOThumb(videoPath, posterPath)
	writeJSON(w, http.StatusOK, map[string]any{
		"video_path":    videoPath,
		"poster_path":   posterPath,
		"poster_exists": fileExists(posterPath),
		"nfo_synced":    synced,
		"nfo_path":      strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo",
		"nfo_exists":    fileExists(strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo"),
		"nfo_status":    nfoStatus,
	})
}

func (a *app) handleProxyCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	rawProxy := strings.TrimSpace(anyToString(payload["proxy"]))
	if rawProxy == "" {
		writeError(w, http.StatusBadRequest, "proxy is required")
		return
	}
	proxyURL, err := normalizeProxyURL(rawProxy)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	timeout, err := parseTimeout(payload["timeout"], 8)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	checks, overallOK, averageLatency := checkProxyLatency(proxyURL, timeout)
	writeJSON(w, http.StatusOK, map[string]any{"proxy": proxyURL, "ok": overallOK, "latency_ms": averageLatency, "checks": checks})
}

func (a *app) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var options jobOptions
	if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body")
		return
	}
	parsed, err := parseJobOptions(options)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.state.start(); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	go a.runJob(parsed)
	writeJSON(w, http.StatusAccepted, a.state.snapshot())
}

func (a *app) staticVersion() string {
	parts := []string{}
	for _, name := range []string{"styles.css", "app.js"} {
		stat, err := os.Stat(filepath.Join(a.staticDir, name))
		if err == nil {
			parts = append(parts, fmt.Sprintf("%s:%d:%d", name, stat.Size(), stat.ModTime().UnixNano()))
		}
	}
	if len(parts) == 0 {
		return "0"
	}
	sum := sha1.Sum([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("%x", sum[:])[:12]
}

func (a *app) preferencesPath() string {
	dir := a.prefsDir
	if _, err := os.Stat(dir); err != nil {
		dir = a.rootDir
	}
	return filepath.Join(dir, preferencesFile)
}

func (a *app) loadPreferences() preferences {
	prefs := preferences{Target: defaultTarget}
	raw, err := os.ReadFile(a.preferencesPath())
	if err != nil {
		return prefs
	}
	_ = json.Unmarshal(raw, &prefs)
	prefs.Target = strings.TrimSpace(prefs.Target)
	if prefs.Target == "" {
		prefs.Target = defaultTarget
	}
	return prefs
}

func (a *app) savePreferences(p preferences) error {
	if err := os.MkdirAll(filepath.Dir(a.preferencesPath()), 0o755); err != nil {
		return err
	}
	body, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(a.preferencesPath(), append(body, '\n'), 0o644)
}

func parseBool(value string, defaultValue bool) bool {
	if strings.TrimSpace(value) == "" {
		return defaultValue
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseTimeout(value any, defaultValue int) (int, error) {
	if value == nil || fmt.Sprint(value) == "" {
		return defaultValue, nil
	}
	timeout, err := strconv.Atoi(fmt.Sprint(value))
	if err != nil {
		return 0, errors.New("timeout must be an integer")
	}
	if timeout < 5 || timeout > 120 {
		return 0, errors.New("timeout must be between 5 and 120 seconds")
	}
	return timeout, nil
}

func parsePosterStyle(value string) (string, error) {
	style := strings.ToLower(strings.TrimSpace(value))
	if style == "" {
		style = "basename"
	}
	if style != "basename" && style != "folder" {
		return "", errors.New("poster_style must be 'basename' or 'folder'")
	}
	return style, nil
}

func normalizeProxyURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("proxy must be a valid URL, e.g. http://127.0.0.1:7890")
	}
	return raw, nil
}

func parseJobOptions(options jobOptions) (jobOptions, error) {
	options.Target = strings.TrimSpace(options.Target)
	if options.Target == "" {
		return options, errors.New("target is required")
	}
	posterStyle, err := parsePosterStyle(options.PosterStyle)
	if err != nil {
		return options, err
	}
	timeout, err := parseTimeout(options.Timeout, 20)
	if err != nil {
		return options, err
	}
	proxyURL, err := normalizeProxyURL(options.Proxy)
	if err != nil {
		return options, err
	}
	options.PosterStyle = posterStyle
	options.Timeout = timeout
	options.Proxy = proxyURL
	options.AIProvider = normalizeAIProvider(options.AIProvider)
	options.AIModel = resolveAIModel(options.AIProvider, options.AIModel)
	options.AIBaseURL = resolveAIBaseURL(options.AIProvider, options.AIBaseURL)
	return options, nil
}

func normalizeAIProvider(raw string) string {
	provider := strings.ToLower(strings.TrimSpace(raw))
	if provider == "" {
		provider = defaultAIProvider
	}
	if _, ok := aiProviderPresets[provider]; ok {
		return provider
	}
	return defaultAIProvider
}

func resolveAIModel(provider, model string) string {
	model = strings.TrimSpace(model)
	if model != "" {
		return model
	}
	preset := aiProviderPresets[provider]
	if preset.Model != "" {
		return preset.Model
	}
	return defaultAIModel
}

func resolveAIBaseURL(provider, baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL != "" {
		return strings.TrimRight(baseURL, "/")
	}
	preset := aiProviderPresets[provider]
	if preset.BaseURL != "" {
		return preset.BaseURL
	}
	return aiProviderPresets[defaultAIProvider].BaseURL
}

func parseAIOptions(payload map[string]any) jobOptions {
	provider := normalizeAIProvider(anyToString(payload["ai_provider"]))
	return jobOptions{
		AIProvider: provider,
		AIAPIKey:   strings.TrimSpace(anyToString(payload["ai_api_key"])),
		AIModel:    resolveAIModel(provider, anyToString(payload["ai_model"])),
		AIBaseURL:  resolveAIBaseURL(provider, anyToString(payload["ai_base_url"])),
	}
}

func validateVideoPath(raw any) (string, error) {
	value := strings.TrimSpace(fmt.Sprint(raw))
	if value == "" {
		return "", errors.New("video_path is required")
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(abs)
	if err != nil || info.IsDir() {
		return "", errors.New("video_path does not exist")
	}
	if !videoExtensions[strings.ToLower(filepath.Ext(abs))] {
		return "", errors.New("video_path is not a supported video file")
	}
	return abs, nil
}

func buildFileItem(videoPath, posterStyle string) fileItem {
	parsed := inferTrackFromPath(videoPath)
	posterPath := resolvePosterPath(videoPath, posterStyle)
	nfoPath := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo"
	item := fileItem{
		VideoPath:    videoPath,
		FileName:     filepath.Base(videoPath),
		Parsed:       parsed != nil,
		PosterPath:   posterPath,
		PosterExists: fileExists(posterPath),
		NFOPath:      nfoPath,
		NFOExists:    fileExists(nfoPath),
	}
	if parsed != nil {
		item.Artist = parsed.Artist
		item.Title = parsed.Title
	}
	return item
}

func normalizeOptionalQuery(value string) string {
	text := strings.TrimSpace(value)
	if text == "" {
		return ""
	}
	if ignoredQueryValues[strings.ToLower(text)] {
		return ""
	}
	return text
}

func collectVideoFiles(target string, recursive bool) []string {
	info, err := os.Stat(target)
	if err != nil {
		return nil
	}
	files := []string{}
	if !info.IsDir() {
		if videoExtensions[strings.ToLower(filepath.Ext(target))] {
			return []string{target}
		}
		return nil
	}
	if recursive {
		_ = filepath.WalkDir(target, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && videoExtensions[strings.ToLower(filepath.Ext(path))] {
				files = append(files, path)
			}
			return nil
		})
	} else {
		entries, _ := os.ReadDir(target)
		for _, entry := range entries {
			if !entry.IsDir() {
				path := filepath.Join(target, entry.Name())
				if videoExtensions[strings.ToLower(filepath.Ext(path))] {
					files = append(files, path)
				}
			}
		}
	}
	sort.Strings(files)
	return files
}

func resolvePosterPath(videoPath, posterStyle string) string {
	if posterStyle == "folder" {
		return filepath.Join(filepath.Dir(videoPath), "poster.jpg")
	}
	return filepath.Join(filepath.Dir(videoPath), strings.TrimSuffix(filepath.Base(videoPath), filepath.Ext(videoPath))+"-poster.jpg")
}

func normalizeText(text string) string {
	text = strings.ReplaceAll(text, "_", " ")
	text = strings.ReplaceAll(text, ".", " ")
	return strings.Join(strings.Fields(text), " ")
}

func cleanComponent(text string) string {
	text = strings.Trim(normalizeText(text), "-_| ")
	return strings.Join(strings.Fields(text), " ")
}

func removeNoiseFragments(text string) string {
	cleaned := text
	bracketRe := regexp.MustCompile(`\[[^\]]+\]|\([^\)]+\)`)
	cleaned = bracketRe.ReplaceAllStringFunc(cleaned, func(fragment string) string {
		body := strings.ToLower(fragment[1 : len(fragment)-1])
		for _, keyword := range append(slices.Clone(noiseKeywords), platformKeywords...) {
			if strings.Contains(body, keyword) {
				return " "
			}
		}
		return fragment
	})
	lowered := strings.ToLower(cleaned)
	for _, keyword := range noiseKeywords {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(keyword) + `\b`)
		cleaned = re.ReplaceAllString(cleaned, " ")
		lowered = strings.ToLower(cleaned)
		_ = lowered
	}
	for {
		next := cleaned
		for _, keyword := range platformKeywords {
			re := regexp.MustCompile(`(?i)(?:\s*[-–—|_/]+\s*)?` + regexp.QuoteMeta(keyword) + `\s*$`)
			next = re.ReplaceAllString(next, " ")
		}
		next = strings.Join(strings.Fields(strings.Trim(next, "-_| ")), " ")
		if next == strings.Join(strings.Fields(strings.Trim(cleaned, "-_| ")), " ") {
			return next
		}
		cleaned = next
	}
}

func parseArtistTitle(candidate string) *parsedTrack {
	raw := candidate
	candidate = normalizeText(candidate)
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`^\[(?P<artist>[^\]]+)\]\s*(?P<title>.+)$`),
		regexp.MustCompile(`^(?P<artist>.+?)\s*[-–—]\s*(?P<title>.+)$`),
		regexp.MustCompile(`(?i)^(?P<title>.+?)\s+by\s+(?P<artist>.+)$`),
	}
	for _, pattern := range patterns {
		match := pattern.FindStringSubmatch(candidate)
		if match == nil {
			continue
		}
		groups := map[string]string{}
		for i, name := range pattern.SubexpNames() {
			if i > 0 && name != "" {
				groups[name] = match[i]
			}
		}
		artist := cleanComponent(groups["artist"])
		title := removeNoiseFragments(cleanComponent(groups["title"]))
		if artist != "" && title != "" {
			return &parsedTrack{Artist: artist, Title: title, Raw: raw}
		}
	}
	return nil
}

func inferTrackFromPath(videoPath string) *parsedTrack {
	stem := strings.TrimSuffix(filepath.Base(videoPath), filepath.Ext(videoPath))
	candidates := []string{stem, filepath.Base(filepath.Dir(videoPath)), filepath.Base(filepath.Dir(videoPath)) + " - " + stem}
	for _, candidate := range candidates {
		if parsed := parseArtistTitle(candidate); parsed != nil {
			return parsed
		}
	}
	return nil
}

type posterSearchResult struct {
	Query      string       `json:"query"`
	Parsed     *parsedTrack `json:"parsed,omitempty"`
	Candidates []candidate  `json:"candidates"`
}

func (a *app) buildPosterCandidates(videoPath, query string, timeout int, proxyURL string) posterSearchResult {
	parsed := inferTrackFromPath(videoPath)
	searchTitle := strings.TrimSpace(query)
	searchArtist := ""
	baseQuery := searchTitle
	if searchTitle == "" && parsed != nil {
		searchArtist = parsed.Artist
		searchTitle = removeNoiseFragments(parsed.Title)
		baseQuery = strings.TrimSpace(searchArtist + " " + searchTitle)
	}
	if baseQuery == "" {
		baseQuery = removeNoiseFragments(strings.TrimSuffix(filepath.Base(videoPath), filepath.Ext(videoPath)))
	}
	candidates := []candidate{}
	addSourceCandidates := func(items []map[string]string, source string, priority int) {
		for idx, item := range items {
			imageURL := strings.TrimSpace(item["image_url"])
			if imageURL == "" {
				continue
			}
			title := strings.TrimSpace(item["title"])
			if title == "" {
				title = source
			}
			candidates = append(candidates, candidate{
				ID:         fmt.Sprintf("%s-%d", strings.ToLower(source), idx),
				Source:     source,
				Title:      title,
				Subtitle:   item["subtitle"],
				ImageURL:   imageURL,
				WebpageURL: item["webpage_url"],
				SortScore:  priority,
			})
		}
	}

	addSourceCandidates(searchLgychCandidates(searchArtist, searchTitle, timeout, proxyURL), "lgych.com", 600)
	addSourceCandidates(searchBugsCandidates(searchArtist, searchTitle, timeout, proxyURL), "Bugs", 560)
	addSourceCandidates(searchITunesCandidates(searchArtist, searchTitle, timeout, proxyURL), "iTunes", 300)
	addSourceCandidates(searchDeezerCandidates(searchArtist, searchTitle, timeout, proxyURL), "Deezer", 260)
	addSourceCandidates(searchAudioDBCandidates(searchArtist, searchTitle, timeout, proxyURL), "AudioDB", 240)
	deduped := map[string]candidate{}
	for _, item := range candidates {
		key := strings.ToLower(item.ImageURL)
		current, ok := deduped[key]
		if !ok || item.SortScore > current.SortScore {
			deduped[key] = item
		}
	}
	result := make([]candidate, 0, len(deduped))
	for _, item := range deduped {
		result = append(result, item)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].SortScore == result[j].SortScore {
			return result[i].Title < result[j].Title
		}
		return result[i].SortScore > result[j].SortScore
	})
	if len(result) > 16 {
		result = result[:16]
	}
	var parsedCopy *parsedTrack
	if parsed != nil {
		clone := *parsed
		parsedCopy = &clone
	}
	return posterSearchResult{Query: baseQuery, Parsed: parsedCopy, Candidates: result}
}

func searchITunesCandidates(artist, title string, timeout int, proxyURL string) []map[string]string {
	if strings.TrimSpace(artist+title) == "" {
		return nil
	}
	endpoint := "https://itunes.apple.com/search?entity=song&limit=8&term=" + url.QueryEscape(strings.TrimSpace(artist+" "+title))
	payload := map[string]any{}
	if err := getJSON(endpoint, timeout, proxyURL, &payload); err != nil {
		return nil
	}
	rawResults, _ := payload["results"].([]any)
	results := []map[string]string{}
	for _, raw := range rawResults {
		row, _ := raw.(map[string]any)
		artwork := upscaleITunesArtwork(anyToString(row["artworkUrl100"]))
		if artwork == "" {
			continue
		}
		trackName := removeNoiseFragments(anyToString(row["trackName"]))
		artistName := anyToString(row["artistName"])
		titleText := strings.Trim(strings.TrimSpace(trackName+" - "+artistName), " -")
		results = append(results, map[string]string{
			"title":     titleText,
			"subtitle":  anyToString(row["collectionName"]),
			"image_url": artwork,
		})
	}
	return results
}

func searchDeezerCandidates(artist, title string, timeout int, proxyURL string) []map[string]string {
	query := title
	if strings.TrimSpace(artist) != "" {
		query = fmt.Sprintf(`artist:"%s" track:"%s"`, artist, title)
	}
	endpoint := "https://api.deezer.com/search?q=" + url.QueryEscape(query) + "&limit=6"
	payload := map[string]any{}
	if err := getJSON(endpoint, timeout, proxyURL, &payload); err != nil {
		return nil
	}
	rawResults, _ := payload["data"].([]any)
	results := []map[string]string{}
	for _, raw := range rawResults {
		row, _ := raw.(map[string]any)
		album, _ := row["album"].(map[string]any)
		artistInfo, _ := row["artist"].(map[string]any)
		imageURL := anyToString(album["cover_xl"])
		if imageURL == "" {
			imageURL = anyToString(album["cover_big"])
		}
		if imageURL == "" {
			imageURL = anyToString(album["cover_medium"])
		}
		if imageURL == "" {
			continue
		}
		titleText := strings.Trim(strings.TrimSpace(anyToString(row["title"])+" - "+anyToString(artistInfo["name"])), " -")
		results = append(results, map[string]string{"title": titleText, "subtitle": anyToString(album["title"]), "image_url": imageURL})
	}
	return results
}

func searchAudioDBCandidates(artist, title string, timeout int, proxyURL string) []map[string]string {
	endpoint := "https://www.theaudiodb.com/api/v1/json/2/searchtrack.php?t=" + url.QueryEscape(title)
	if strings.TrimSpace(artist) != "" {
		endpoint += "&s=" + url.QueryEscape(artist)
	}
	payload := map[string]any{}
	if err := getJSON(endpoint, timeout, proxyURL, &payload); err != nil {
		return nil
	}
	rawResults, _ := payload["track"].([]any)
	results := []map[string]string{}
	for _, raw := range rawResults {
		row, _ := raw.(map[string]any)
		imageURL := anyToString(row["strTrackThumb"])
		if imageURL == "" {
			imageURL = anyToString(row["strAlbumThumb"])
		}
		if imageURL == "" {
			continue
		}
		titleText := strings.Trim(strings.TrimSpace(anyToString(row["strTrack"])+" - "+anyToString(row["strArtist"])), " -")
		results = append(results, map[string]string{"title": titleText, "subtitle": anyToString(row["strAlbum"]), "image_url": imageURL})
	}
	return results
}

func searchBugsCandidates(artist, title string, timeout int, proxyURL string) []map[string]string {
	query := strings.TrimSpace(strings.TrimSpace(artist) + " " + strings.TrimSpace(title))
	if query == "" {
		return nil
	}
	endpoint := "https://music.bugs.co.kr/search/integrated?q=" + url.QueryEscape(query)
	text, err := getText(endpoint, timeout, proxyURL, map[string]string{
		"User-Agent":      "Mozilla/5.0",
		"Referer":         "https://music.bugs.co.kr/",
		"Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
	})
	if err != nil {
		return nil
	}
	rowRe := regexp.MustCompile(`(?is)<tr\b[^>]*rowType="track"[^>]*>.*?</tr>`)
	mvIDRe := regexp.MustCompile(`(?i)\bmvId="([^"]+)"`)
	trackIDRe := regexp.MustCompile(`(?i)\btrackId="([^"]+)"`)
	imageRe := regexp.MustCompile(`(?i)<img[^>]+src="([^"]+)"`)
	titleRe := regexp.MustCompile(`(?is)<p\s+class="title"[^>]*>.*?<a[^>]+title="([^"]+)"`)
	artistRe := regexp.MustCompile(`(?is)<p\s+class="artist"[^>]*>.*?<a[^>]+title="([^"]+)"`)
	albumRe := regexp.MustCompile(`(?i)class="album"\s+title="([^"]+)"`)
	results := []map[string]string{}
	seen := map[string]bool{}
	for _, row := range rowRe.FindAllString(text, -1) {
		mv := firstSubmatch(mvIDRe, row)
		if mv == "" || mv == "0" {
			continue
		}
		imageURL := normalizeBugsImageURL(html.UnescapeString(firstSubmatch(imageRe, row)))
		if imageURL == "" || seen[strings.ToLower(imageURL)] {
			continue
		}
		titleText := cleanComponent(html.UnescapeString(firstSubmatch(titleRe, row)))
		artistName := cleanComponent(html.UnescapeString(firstSubmatch(artistRe, row)))
		subtitle := cleanComponent(html.UnescapeString(firstSubmatch(albumRe, row)))
		trackID := firstSubmatch(trackIDRe, row)
		if titleText == "" {
			continue
		}
		seen[strings.ToLower(imageURL)] = true
		results = append(results, map[string]string{
			"title":       strings.Trim(strings.TrimSpace(titleText+" - "+artistName), " -"),
			"subtitle":    subtitle,
			"image_url":   imageURL,
			"webpage_url": ternary(trackID != "", "https://music.bugs.co.kr/track/"+trackID, ""),
		})
		if len(results) >= 8 {
			break
		}
	}
	return results
}

func searchLgychCandidates(artist, title string, timeout int, proxyURL string) []map[string]string {
	query := strings.TrimSpace(strings.TrimSpace(artist) + " " + strings.TrimSpace(title))
	if query == "" {
		return nil
	}
	endpoint := "https://www.lgych.com/?s=" + url.QueryEscape(query)
	text, err := getText(endpoint, timeout, proxyURL, map[string]string{"User-Agent": "Mozilla/5.0", "Referer": "https://www.lgych.com/"})
	if err != nil {
		return nil
	}
	imgRe := regexp.MustCompile(`(?is)<img[^>]*class="[^"]*thumb[^"]*"[^>]*>`)
	results := []map[string]string{}
	seen := map[string]bool{}
	for _, tag := range imgRe.FindAllString(text, -1) {
		rawImage := attrValue(tag, "data-src")
		if rawImage == "" {
			rawImage = attrValue(tag, "src")
		}
		imageURL := normalizeLgychImageURL(rawImage)
		titleText := sanitizeLgychTitle(attrValue(tag, "alt"))
		lowered := strings.ToLower(imageURL)
		if imageURL == "" || titleText == "" || seen[lowered] {
			continue
		}
		if !strings.Contains(lowered, "/wp-content/uploads/") || strings.Contains(lowered, "thumb-ing.gif") || strings.Contains(lowered, "/logo") || strings.Contains(lowered, "cropped-") || strings.Contains(lowered, "weixin") || strings.Contains(lowered, "qrcode") {
			continue
		}
		seen[lowered] = true
		results = append(results, map[string]string{"title": titleText, "subtitle": "lgych.com", "image_url": imageURL})
		if len(results) >= 8 {
			break
		}
	}
	return results
}

func checkProxyLatency(proxyURL string, timeout int) ([]map[string]any, bool, any) {
	targets := []struct{ Name, URL string }{{"iTunes", "https://itunes.apple.com/search?term=test&entity=song&limit=1"}, {"YouTube", "https://www.youtube.com/generate_204"}, {"lgych", "https://www.lgych.com/"}}
	results := []map[string]any{}
	transport := &http.Transport{Proxy: mustProxy(proxyURL)}
	client := &http.Client{Timeout: time.Duration(timeout) * time.Second, Transport: transport}
	latencies := []float64{}
	for _, target := range targets {
		started := time.Now()
		req, _ := http.NewRequest(http.MethodGet, target.URL, nil)
		req.Header.Set("User-Agent", "mv-metadata-studio/1.0")
		resp, err := client.Do(req)
		elapsed := math.Round(float64(time.Since(started).Milliseconds())*10) / 10
		if err != nil {
			results = append(results, map[string]any{"name": target.Name, "ok": false, "latency_ms": elapsed, "error": err.Error()})
			continue
		}
		_ = resp.Body.Close()
		ok := resp.StatusCode < 400
		if ok {
			latencies = append(latencies, elapsed)
		}
		results = append(results, map[string]any{"name": target.Name, "ok": ok, "latency_ms": elapsed, "status_code": resp.StatusCode})
	}
	overallOK := len(results) > 0
	for _, item := range results {
		if ok, _ := item["ok"].(bool); !ok {
			overallOK = false
		}
	}
	if len(latencies) == 0 {
		return results, overallOK, nil
	}
	total := 0.0
	for _, latency := range latencies {
		total += latency
	}
	average := math.Round((total/float64(len(latencies)))*10) / 10
	return results, overallOK, average
}

func (a *app) ensureNFOAfterManualApply(videoPath, posterStyle string, timeout int, proxyURL string, opts jobOptions) (string, error) {
	nfoPath := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo"
	if fileExists(nfoPath) {
		return "existing", nil
	}
	parsed := inferTrackFromPath(videoPath)
	title := strings.TrimSuffix(filepath.Base(videoPath), filepath.Ext(videoPath))
	artist := "Unknown Artist"
	if parsed != nil {
		title = parsed.Title
		artist = parsed.Artist
	}
	posterPath := resolvePosterPath(videoPath, posterStyle)
	meta := metadata{Artist: artist, Title: title, Genre: "Music", ThumbURL: posterPath, Tagline: title}
	if plot := a.buildPlot(parsed, nil, opts, timeout, proxyURL); plot != "" {
		meta.Plot = plot
	}
	if err := writeNFO(meta, nfoPath, filepath.Base(posterPath)); err != nil {
		return "", err
	}
	return "minimal", nil
}

func syncNFOThumb(videoPath, posterPath string) bool {
	nfoPath := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo"
	raw, err := os.ReadFile(nfoPath)
	if err != nil {
		return false
	}
	type node struct {
		XMLName xml.Name
		Content []byte `xml:",innerxml"`
	}
	var root node
	if err := xml.Unmarshal(raw, &root); err != nil {
		return false
	}
	inner := string(root.Content)
	thumbRe := regexp.MustCompile(`(?is)<thumb>.*?</thumb>`)
	replacement := "<thumb>" + xmlEscape(filepath.Base(posterPath)) + "</thumb>"
	if thumbRe.MatchString(inner) {
		inner = thumbRe.ReplaceAllString(inner, replacement)
	} else {
		inner += replacement
	}
	titleRe := regexp.MustCompile(`(?is)<title>(.*?)</title>`)
	inner = titleRe.ReplaceAllStringFunc(inner, func(fragment string) string {
		match := titleRe.FindStringSubmatch(fragment)
		if len(match) < 2 {
			return fragment
		}
		return "<title>" + xmlEscape(removeNoiseFragments(html.UnescapeString(match[1]))) + "</title>"
	})
	output := []byte(xml.Header + "<" + root.XMLName.Local + ">" + inner + "</" + root.XMLName.Local + ">")
	return os.WriteFile(nfoPath, output, 0o644) == nil
}

func (a *app) runJob(options jobOptions) {
	defer func() {
		a.state.mu.Lock()
		a.state.running = false
		a.state.finishedAt = time.Now().Format(time.RFC3339)
		a.state.currentFile = ""
		a.state.mu.Unlock()
	}()

	files := collectVideoFiles(options.Target, options.Recursive)
	a.state.mu.Lock()
	a.state.stats.Scanned = len(files)
	a.state.mu.Unlock()
	a.state.appendLog(fmt.Sprintf("Job started. target=%s total_files=%d", options.Target, len(files)))
	for idx, videoPath := range files {
		a.state.mu.Lock()
		a.state.currentFile = videoPath
		a.state.stats.Processed = idx
		a.state.mu.Unlock()
		a.state.appendLog(fmt.Sprintf("[%d/%d] Processing %s", idx+1, len(files), filepath.Base(videoPath)))
		status := a.processVideo(videoPath, options)
		a.state.mu.Lock()
		switch status {
		case "success":
			a.state.stats.Success++
		case "skipped":
			a.state.stats.Skipped++
		default:
			a.state.stats.Failed++
		}
		a.state.stats.Processed = idx + 1
		a.state.mu.Unlock()
	}
}

func (a *app) processVideo(videoPath string, options jobOptions) string {
	nfoPath := strings.TrimSuffix(videoPath, filepath.Ext(videoPath)) + ".nfo"
	posterPath := resolvePosterPath(videoPath, options.PosterStyle)
	if fileExists(nfoPath) && !options.Overwrite {
		return "skipped"
	}
	parsed := inferTrackFromPath(videoPath)
	if parsed == nil {
		return "failed"
	}
	if options.DryRun {
		return "success"
	}
	timeout := options.Timeout
	itunes := searchITunesCandidates(parsed.Artist, parsed.Title, timeout, options.Proxy)
	meta := metadata{
		Artist:  parsed.Artist,
		Title:   parsed.Title,
		Genre:   "Music",
		Tagline: parsed.Title,
	}
	if len(itunes) > 0 {
		meta.Album = itunes[0]["subtitle"]
		meta.ThumbURL = itunes[0]["image_url"]
	}
	if plot := a.buildPlot(parsed, itunes, options, timeout, options.Proxy); plot != "" {
		meta.Plot = plot
	}
	if meta.ThumbURL == "" {
		candidates := a.buildPosterCandidates(videoPath, "", timeout, options.Proxy).Candidates
		if len(candidates) > 0 {
			meta.ThumbURL = candidates[0].ImageURL
		}
	}
	posterExists := fileExists(posterPath)
	if meta.ThumbURL != "" && (options.Overwrite || !posterExists) {
		ok, err := downloadPoster(meta.ThumbURL, posterPath, timeout, options.Proxy)
		if err != nil {
			a.state.appendLog(fmt.Sprintf("Poster download failed (%s): %v", filepath.Base(videoPath), err))
		} else if ok {
			posterExists = true
		}
	}
	if err := writeNFO(meta, nfoPath, ternary(posterExists, filepath.Base(posterPath), "")); err != nil {
		a.state.appendLog(fmt.Sprintf("Failed to write nfo for %s: %v", filepath.Base(videoPath), err))
		return "failed"
	}
	return "success"
}

func (a *app) buildPlot(parsed *parsedTrack, itunes []map[string]string, options jobOptions, timeout int, proxyURL string) string {
	if parsed == nil {
		return ""
	}
	if strings.TrimSpace(options.AIAPIKey) != "" || options.AIProvider == "custom" {
		prompt := fmt.Sprintf("请为 MV 生成一段 80 字以内的中文简介，包含歌手和歌曲名，不要编造。歌手：%s，歌曲：%s。", parsed.Artist, parsed.Title)
		if result := callAI(prompt, options, timeout, proxyURL); result != "" {
			return result
		}
	}
	if len(itunes) > 0 && strings.TrimSpace(itunes[0]["subtitle"]) != "" {
		return fmt.Sprintf("《%s》由 %s 演唱，收录于 %s。", parsed.Title, parsed.Artist, itunes[0]["subtitle"])
	}
	return fmt.Sprintf("《%s》由 %s 演唱。", parsed.Title, parsed.Artist)
}

func callAI(prompt string, options jobOptions, timeout int, proxyURL string) string {
	endpoint := strings.TrimRight(options.AIBaseURL, "/") + "/chat/completions"
	payload := map[string]any{
		"model":       options.AIModel,
		"messages":    []map[string]string{{"role": "system", "content": "你是一个简洁的中文音乐资料编辑助手。"}, {"role": "user", "content": prompt}},
		"temperature": 0.3,
	}
	body, _ := json.Marshal(payload)
	client := newHTTPClient(timeout, proxyURL)
	req, _ := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(options.AIAPIKey) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(options.AIAPIKey))
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return ""
	}
	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}
	choices, _ := result["choices"].([]any)
	if len(choices) == 0 {
		return ""
	}
	first, _ := choices[0].(map[string]any)
	message, _ := first["message"].(map[string]any)
	return strings.TrimSpace(anyToString(message["content"]))
}

func writeNFO(meta metadata, nfoPath, posterFileName string) error {
	type uniqueID struct {
		XMLName xml.Name `xml:"uniqueid"`
		Type    string   `xml:"type,attr,omitempty"`
		Default string   `xml:"default,attr,omitempty"`
		Value   string   `xml:",chardata"`
	}
	type musicVideo struct {
		XMLName           xml.Name  `xml:"musicvideo"`
		Title             string    `xml:"title,omitempty"`
		Artist            string    `xml:"artist,omitempty"`
		Album             string    `xml:"album,omitempty"`
		Plot              string    `xml:"plot,omitempty"`
		Tagline           string    `xml:"tagline,omitempty"`
		Premiered         string    `xml:"premiered,omitempty"`
		Year              string    `xml:"year,omitempty"`
		Studio            string    `xml:"studio,omitempty"`
		Genre             string    `xml:"genre,omitempty"`
		Rating            string    `xml:"rating,omitempty"`
		UserRating        string    `xml:"userrating,omitempty"`
		Votes             string    `xml:"votes,omitempty"`
		Runtime           string    `xml:"runtime,omitempty"`
		DurationInSeconds string    `xml:"durationinseconds,omitempty"`
		Trailer           string    `xml:"trailer,omitempty"`
		UniqueID          *uniqueID `xml:"uniqueid,omitempty"`
		Thumb             string    `xml:"thumb,omitempty"`
		Source            string    `xml:"source,omitempty"`
	}
	document := musicVideo{Title: meta.Title, Artist: meta.Artist, Album: meta.Album, Plot: meta.Plot, Tagline: meta.Tagline, Premiered: meta.Premiered, Year: meta.Year, Studio: meta.Studio, Genre: ternary(meta.Genre != "", meta.Genre, "Music"), Trailer: meta.YoutubeURL, Thumb: posterFileName, Source: "mv-metadata-studio"}
	if meta.Rating != nil {
		document.Rating = fmt.Sprintf("%.1f", *meta.Rating)
	}
	if meta.UserRating != nil {
		document.UserRating = strconv.Itoa(*meta.UserRating)
	}
	if meta.Votes != nil {
		document.Votes = strconv.Itoa(*meta.Votes)
	}
	if meta.DurationSeconds > 0 {
		document.Runtime = strconv.Itoa(max(1, int(math.Round(float64(meta.DurationSeconds)/60))))
		document.DurationInSeconds = strconv.Itoa(meta.DurationSeconds)
	}
	if meta.YoutubeID != "" {
		document.UniqueID = &uniqueID{Type: "youtube", Default: "true", Value: meta.YoutubeID}
	}
	body, err := xml.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(nfoPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(nfoPath, append([]byte(xml.Header), body...), 0o644)
}

func downloadPoster(imageURL, outputPath string, timeout int, proxyURL string) (bool, error) {
	for _, candidateURL := range iterDownloadURLs(imageURL) {
		body, err := downloadBytes(candidateURL, timeout, proxyURL)
		if err != nil {
			if proxyURL != "" {
				body, err = downloadBytes(candidateURL, timeout, "")
			}
			if err != nil {
				continue
			}
		}
		if _, _, err := image.DecodeConfig(bytes.NewReader(body)); err != nil {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
			return false, err
		}
		if err := os.WriteFile(outputPath, body, 0o644); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func iterDownloadURLs(imageURL string) []string {
	urls := []string{imageURL}
	if strings.Contains(strings.ToLower(imageURL), "lgych.com/wp-content/uploads/") {
		timthumb := "https://www.lgych.com/wp-content/themes/modown/timthumb.php?src=" + url.QueryEscape(imageURL) + "&w=800&h=800&zc=1"
		urls = append(urls, timthumb)
	}
	return urls
}

func getJSON(endpoint string, timeout int, proxyURL string, out any) error {
	client := newHTTPClient(timeout, proxyURL)
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("User-Agent", "mv-metadata-studio/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func getText(endpoint string, timeout int, proxyURL string, headers map[string]string) (string, error) {
	client := newHTTPClient(timeout, proxyURL)
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	req.Header.Set("User-Agent", "mv-metadata-studio/1.0")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("http %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func downloadBytes(endpoint string, timeout int, proxyURL string) ([]byte, error) {
	textHeaders := map[string]string{"User-Agent": "Mozilla/5.0", "Referer": "https://www.lgych.com/"}
	client := newHTTPClient(timeout, proxyURL)
	req, _ := http.NewRequest(http.MethodGet, endpoint, nil)
	for key, value := range textHeaders {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func newHTTPClient(timeout int, proxyURL string) *http.Client {
	transport := &http.Transport{}
	if strings.TrimSpace(proxyURL) != "" {
		transport.Proxy = mustProxy(proxyURL)
	}
	return &http.Client{Timeout: time.Duration(timeout) * time.Second, Transport: transport}
}

func mustProxy(proxyURL string) func(*http.Request) (*url.URL, error) {
	parsed, _ := url.Parse(proxyURL)
	return http.ProxyURL(parsed)
}

func upscaleITunesArtwork(raw string) string {
	if raw == "" {
		return ""
	}
	re := regexp.MustCompile(`/\d+x\d+bb\.`)
	return re.ReplaceAllString(raw, "/1200x1200bb.")
}

func normalizeBugsImageURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	re := regexp.MustCompile(`/album/images/\d+/`)
	return re.ReplaceAllString(raw, "/album/images/1000/")
}

func normalizeLgychImageURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	parsed, err := url.Parse(raw)
	if err == nil && strings.Contains(strings.ToLower(parsed.Path), "timthumb.php") {
		src := parsed.Query().Get("src")
		if src != "" {
			decoded, _ := url.QueryUnescape(src)
			return decoded
		}
	}
	return raw
}

func sanitizeLgychTitle(raw string) string {
	title := strings.Join(strings.Fields(html.UnescapeString(raw)), " ")
	lowered := strings.ToLower(title)
	if title == "" || strings.Contains(title, "<") || strings.Contains(title, ">") || strings.Contains(lowered, "<img") || strings.Contains(lowered, " href=") {
		return ""
	}
	return title
}

func attrValue(tag, attr string) string {
	doubleQuoted := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(attr) + `\s*=\s*"([^"]*)"`)
	if value := firstSubmatch(doubleQuoted, tag); value != "" {
		return html.UnescapeString(value)
	}
	singleQuoted := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(attr) + `\s*=\s*'([^']*)'`)
	return html.UnescapeString(firstSubmatch(singleQuoted, tag))
}

func firstSubmatch(re *regexp.Regexp, text string) string {
	match := re.FindStringSubmatch(text)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func anyToString(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func xmlEscape(text string) string {
	var builder strings.Builder
	_ = xml.EscapeText(&builder, []byte(text))
	return builder.String()
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
