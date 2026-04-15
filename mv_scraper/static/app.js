(() => {
  const { createApp, nextTick } = Vue;
  const app = createApp({
    data() {
      const root = document.getElementById("app");
      const defaults = root ? root.dataset : {};

      return {
        cursor: 0,
        pollingId: null,
        pollFailureCount: 0,
        pollRequestInFlight: false,
        pollSession: 0,
        running: false,
        autoScroll: true,
        settingsDrawerOpen: false,
        startingJob: false,
        loadingFiles: false,
        checkingProxy: false,
        savingPreferences: false,

        noticeText: "",
        noticeType: "",
        posterNoticeText: "",
        posterNoticeType: "",
        proxyStatusText: "",
        proxyStatusType: "",
        preferencesNoticeText: "",
        preferencesNoticeType: "",

        logs: [],
        posterFiles: [],
        posterFilesLoadedKey: "",
        candidateDialog: null,
        activeScrapeFilter: "pending",
        currentPage: 1,
        pageSize: 6,

        status: {
          scanned: 0,
          processed: 0,
          success: 0,
          skipped: 0,
          failed: 0,
          current_file: "",
          started_at: "",
          finished_at: "",
        },

        form: {
          target: String(defaults.defaultTarget || "").trim(),
          ai_provider: String(defaults.defaultAiProvider || "openai").trim() || "openai",
          ai_api_key: "",
          ai_model: String(defaults.defaultAiModel || "").trim(),
          ai_base_url: "",
          proxy: "",
          poster_style: "basename",
          timeout: 20,
          recursive: true,
          overwrite: false,
          dry_run: false,
          verbose: false,
        },
      };
    },

    computed: {

      progressPercent() {
        const scanned = Number(this.status.scanned) || 0;
        const processed = Number(this.status.processed) || 0;
        if (scanned <= 0) {
          return 0;
        }
        return Math.min(100, Math.round((processed / scanned) * 100));
      },

      successRate() {
        const processed = Number(this.status.processed) || 0;
        const success = Number(this.status.success) || 0;
        if (processed <= 0) {
          return "0%";
        }

        return `${Math.min(100, Math.round((success / processed) * 100))}%`;
      },

      parsedLogs() {
        return this.logs.map((line) => {
          const raw = String(line || "");
          const matched = raw.match(/^\s*\[?(\d{2}:\d{2}:\d{2})\]?\s*(.*)$/);

          if (matched) {
            return {
              time: matched[1],
              text: matched[2] || raw,
              raw,
              level: this.detectLevel(raw),
            };
          }

          return {
            time: "",
            text: raw,
            raw,
            level: this.detectLevel(raw),
          };
        });
      },

      candidateDialogDetails() {
        return this.getCandidateDialogState();
      },

      pendingPosterFiles() {
        return this.posterFiles.filter((file) => !this.isScraped(file));
      },

      scrapedPosterFiles() {
        return this.posterFiles.filter((file) => this.isScraped(file));
      },

      filteredPosterFiles() {
        return this.activeScrapeFilter === "scraped" ? this.scrapedPosterFiles : this.pendingPosterFiles;
      },

      totalPages() {
        return Math.max(1, Math.ceil(this.filteredPosterFiles.length / this.pageSize));
      },

      pagedPosterFiles() {
        const start = (this.currentPage - 1) * this.pageSize;
        return this.filteredPosterFiles.slice(start, start + this.pageSize);
      },

      paginationSummary() {
        if (this.filteredPosterFiles.length === 0) {
          return "0 / 0";
        }

        const start = (this.currentPage - 1) * this.pageSize + 1;
        const end = Math.min(this.filteredPosterFiles.length, start + this.pageSize - 1);
        return `${start}-${end} / ${this.filteredPosterFiles.length}`;
      },
    },

    methods: {
      toggleSettingsDrawer() {
        this.settingsDrawerOpen = !this.settingsDrawerOpen;
        if (this.settingsDrawerOpen) {
          this.syncScrollNow();
        }
      },

      setNotice(text, type = "") {
        this.noticeText = text || "";
        this.noticeType = type || "";
      },

      setPosterNotice(text, type = "") {
        this.posterNoticeText = text || "";
        this.posterNoticeType = type || "";
      },

      setProxyStatus(text, type = "") {
        this.proxyStatusText = text || "";
        this.proxyStatusType = type || "";
      },

      setPreferencesNotice(text, type = "") {
        this.preferencesNoticeText = text || "";
        this.preferencesNoticeType = type || "";
      },

      isScraped(file) {
        return Boolean(file?.poster_exists && file?.nfo_exists);
      },

      scrollToWorkspaceTop() {
        nextTick(() => {
          const node = this.$refs.workspaceCard;
          if (node && typeof node.scrollIntoView === "function") {
            node.scrollIntoView({ behavior: "smooth", block: "start" });
          }
        });
      },

      setScrapeFilter(filter) {
        if (filter !== "pending" && filter !== "scraped") {
          return;
        }

        this.activeScrapeFilter = filter;
        this.currentPage = 1;
        this.scrollToWorkspaceTop();
      },

      syncCurrentPage() {
        const maxPage = Math.max(1, Math.ceil(this.filteredPosterFiles.length / this.pageSize));
        if (this.currentPage > maxPage) {
          this.currentPage = maxPage;
        }
      },

      openCandidateDialog(file, candidate) {
        if (!file || !candidate || file.applying || candidate.applying) {
          return;
        }

        this.candidateDialog = {
          videoPath: file.video_path,
          imageUrl: candidate.image_url,
        };
      },

      closeCandidateDialog() {
        this.candidateDialog = null;
      },

      goToPage(page) {
        const nextPage = Math.min(this.totalPages, Math.max(1, Number(page) || 1));
        if (nextPage === this.currentPage) {
          return;
        }

        this.currentPage = nextPage;
        this.scrollToWorkspaceTop();
      },

      goToPreviousPage() {
        this.goToPage(this.currentPage - 1);
      },

      goToNextPage() {
        this.goToPage(this.currentPage + 1);
      },

      getCandidateDialogState() {
        if (!this.candidateDialog) {
          return { file: null, candidate: null };
        }

        const file = this.posterFiles.find((item) => item.video_path === this.candidateDialog.videoPath) || null;
        if (!file) {
          return { file: null, candidate: null };
        }

        const candidate = file.candidates.find((item) => item.image_url === this.candidateDialog.imageUrl) || null;
        return { file, candidate };
      },

      async parseError(response) {
        try {
          const payload = await response.json();
          return payload.error || `HTTP ${response.status}`;
        } catch (_error) {
          return `HTTP ${response.status}`;
        }
      },

      detectLevel(line) {
        const raw = String(line || "");
        if (raw.includes("| ERROR |")) return "error";
        if (raw.includes("| WARNING |")) return "warning";
        if (raw.includes("| INFO |") || raw.includes("| SUCCESS |")) return "info";
        return "info";
      },

      formatTime(isoString) {
        if (!isoString) return "-";
        try {
          const date = new Date(isoString);
          return date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        } catch (_error) {
          return isoString;
        }
      },

      getPosterFilesKey() {
        const common = this.getCommonOptions();
        return [common.target, common.recursive ? "1" : "0", common.poster_style].join("::");
      },

      ensurePosterFilesLoaded() {
        if (this.loadingFiles) {
          return;
        }

        const common = this.getCommonOptions();
        if (!common.target) {
          return;
        }

        const nextKey = this.getPosterFilesKey();
        if (nextKey === this.posterFilesLoadedKey && this.posterFiles.length > 0) {
          return;
        }

        this.loadPosterFiles(false);
      },

      async fetchStatus() {
        const response = await fetch("/api/status");
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();

        this.running = payload.running;
        this.status = payload.stats || {};
        this.status.current_file = payload.current_file || "";
        this.status.started_at = payload.started_at || "";
        this.status.finished_at = payload.finished_at || "";

        return payload;
      },

      async fetchLogs() {
        const response = await fetch(`/api/logs?cursor=${this.cursor}`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = await response.json();

        if (payload.lines) {
          this.logs = [...this.logs, ...payload.lines];
          this.cursor = payload.next_cursor || 0;

          if (this.logs.length > 500) {
            this.logs = this.logs.slice(-400);
          }

          this.syncScrollNow();
        }
      },

      async pollOnce() {
        const session = this.pollSession;
        if (this.pollRequestInFlight) {
          if (session === this.pollSession) {
            this.schedulePolling(250, session);
          }
          return;
        }

        this.pollRequestInFlight = true;

        try {
          const statusPayload = await this.fetchStatus();

          this.pollFailureCount = 0;

          if (session === this.pollSession) {
            this.schedulePolling(statusPayload.running ? 1000 : 4000, session);
          }
        } catch (error) {
          this.pollFailureCount += 1;
          console.warn("Polling failed", error);

          if (session === this.pollSession) {
            const retryDelay = Math.min(15000, 1000 * 2 ** Math.min(this.pollFailureCount - 1, 4));
            this.schedulePolling(retryDelay, session);
          }
        } finally {
          this.pollRequestInFlight = false;
        }
      },

      schedulePolling(delayMs = 1000, session = this.pollSession) {
        if (session !== this.pollSession) {
          return;
        }

        this.stopPolling();
        this.pollingId = window.setTimeout(() => {
          this.pollingId = null;
          this.pollOnce();
        }, delayMs);
      },

      startPolling(delayMs = 0) {
        this.pollSession += 1;
        this.pollFailureCount = 0;
        this.schedulePolling(delayMs, this.pollSession);
      },

      stopPolling() {
        if (this.pollingId) {
          window.clearTimeout(this.pollingId);
          this.pollingId = null;
        }
      },

      clearLogPanel() {
        this.logs = [];
        this.cursor = 0;
      },

      syncScrollNow() {
        if (this.autoScroll && this.$refs.logOutput) {
          nextTick(() => {
            this.$refs.logOutput.scrollTop = this.$refs.logOutput.scrollHeight;
          });
        }
      },

      getCommonOptions() {
        return {
          target: this.form.target,
          recursive: this.form.recursive,
          poster_style: this.form.poster_style,
        };
      },

      async loadPreferences() {
        try {
          const response = await fetch("/api/preferences");
          if (!response.ok) {
            return;
          }

          const payload = await response.json();
          const target = String(payload.target || "").trim();
          if (target) {
            this.form.target = target;
          }
        } catch (_error) {
        }
      },

      async savePreferences() {
        const target = String(this.form.target || "").trim() || "/media";
        this.savingPreferences = true;
        this.setPreferencesNotice("正在保存路径...");

        try {
          const response = await fetch("/api/preferences", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ target }),
          });

          if (!response.ok) {
            const error = await this.parseError(response);
            this.setPreferencesNotice(`保存失败: ${error}`, "error");
            return;
          }

          const payload = await response.json();
          this.form.target = String(payload.target || target).trim() || "/media";
          this.posterFiles = [];
          this.posterFilesLoadedKey = "";
          this.currentPage = 1;
          this.setPreferencesNotice(`路径已保存: ${this.form.target}`, "ok");
        } catch (error) {
          this.setPreferencesNotice(`保存失败: ${error.message}`, "error");
        } finally {
          this.savingPreferences = false;
        }
      },

      async startJob() {
        if (this.running) {
          this.setNotice("任务正在运行中", "error");
          return;
        }

        if (!this.form.target) {
          this.setNotice("请填写扫描路径", "error");
          return;
        }

        this.startingJob = true;
        this.setNotice("正在启动任务...");

        const options = {
          target: this.form.target,
          recursive: this.form.recursive,
          poster_style: this.form.poster_style,
          overwrite: this.form.overwrite,
          timeout: Number(this.form.timeout) || 20,
          dry_run: this.form.dry_run,
          verbose: this.form.verbose,
          proxy: this.form.proxy || null,
          ai_provider: this.form.ai_provider,
          ai_api_key: this.form.ai_api_key || null,
          ai_model: this.form.ai_model || null,
          ai_base_url: this.form.ai_base_url || null,
        };

        try {
          const response = await fetch("/api/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(options),
          });

          if (!response.ok) {
            const error = await this.parseError(response);
            this.setNotice(`启动失败: ${error}`, "error");
            return;
          }

          this.setNotice("任务已启动", "ok");
          this.running = true;
          this.startPolling(0);
        } catch (error) {
          this.setNotice(`启动失败: ${error.message}`, "error");
        } finally {
          this.startingJob = false;
        }
      },

      async checkProxy() {
        if (!this.form.proxy) {
          this.setProxyStatus("请输入代理地址", "error");
          return;
        }

        this.checkingProxy = true;
        this.setProxyStatus("检测中...");

        try {
          const response = await fetch("/api/proxy/check", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              proxy: this.form.proxy,
              timeout: 10,
            }),
          });

          const result = await response.json();

          if (result.ok) {
            this.setProxyStatus(`可用 (延迟 ${result.latency_ms}ms)`, "ok");
          } else {
            const failed = result.checks?.filter((c) => !c.ok).map((c) => c.name).join(", ");
            this.setProxyStatus(`不可用: ${failed || "连接失败"}`, "error");
          }
        } catch (error) {
          this.setProxyStatus(`检测失败: ${error.message}`, "error");
        } finally {
          this.checkingProxy = false;
        }
      },

      enhancePosterFile(file) {
        return {
          ...file,
          searching: false,
          applying: false,
          searchRequestId: 0,
          candidates: [],
          statusLine: "",
          statusType: "",
        };
      },

      findPosterFileIndex(videoPath) {
        return this.posterFiles.findIndex((item) => item.video_path === videoPath);
      },

      refreshPosterFileState(videoPath, getUpdater) {
        const index = this.findPosterFileIndex(videoPath);
        if (index === -1) {
          return;
        }

        const oldFile = this.posterFiles[index];
        const partial = typeof getUpdater === "function" ? getUpdater(oldFile) : getUpdater;
        const newFile = { ...oldFile, ...partial };
        if (newFile === oldFile) {
          return;
        }

        const newPosterFiles = [...this.posterFiles];
        newPosterFiles[index] = newFile;
        this.posterFiles = newPosterFiles;
        this.syncCurrentPage();
      },

      async loadPosterFiles(showMissingTargetError = true) {
        const common = this.getCommonOptions();
        const requestKey = this.getPosterFilesKey();
        if (!common.target) {
          this.posterFiles = [];
          this.posterFilesLoadedKey = "";
          this.activeScrapeFilter = "pending";
          this.currentPage = 1;
          if (showMissingTargetError) {
            this.setPosterNotice("请先填写扫描路径", "error");
          }
          return;
        }

        this.loadingFiles = true;
        this.setPosterNotice("正在扫描 MV 文件...");

        const params = new URLSearchParams({
          target: common.target,
          recursive: String(common.recursive),
          poster_style: common.poster_style,
        });

        try {
          const response = await fetch(`/api/files?${params.toString()}`);
          if (!response.ok) {
            this.setPosterNotice(`读取失败: ${await this.parseError(response)}`, "error");
            return;
          }

          const body = await response.json();
          const files = Array.isArray(body.files) ? body.files : [];
          this.posterFiles = files.map((file) => this.enhancePosterFile(file));
          this.posterFilesLoadedKey = requestKey;
          this.activeScrapeFilter = "pending";
          this.currentPage = 1;
          this.setPosterNotice(`已载入 ${body.count || 0} 个 MV 文件`, "ok");
        } catch (error) {
          this.setPosterNotice(`加载失败: ${error.message}`, "error");
        } finally {
          this.loadingFiles = false;
        }
      },

      async searchPoster(file) {
        if (file.searching || file.applying) {
          return;
        }

        const videoPath = file.video_path;
        const requestId = Number(file.searchRequestId || 0) + 1;
        this.refreshPosterFileState(videoPath, (current) => ({
          ...current,
          searchRequestId: requestId,
          searching: true,
          candidates: [],
          statusLine: "搜索中...",
          statusType: "",
        }));

        const payload = {
          video_path: videoPath,
          timeout: Number(this.form.timeout) || 20,
          proxy: this.form.proxy || null,
        };

        try {
          const fetchUrl = "/api/poster/search?_t=" + Date.now();
          const response = await fetch(fetchUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });

          if (!response.ok) {
            const error = await this.parseError(response);
            this.refreshPosterFileState(videoPath, (current) => {
              if (current.searchRequestId !== requestId) {
                return current;
              }

              return {
                ...current,
                statusLine: error,
                statusType: "error",
              };
            });
            return;
          }

          const body = await response.json();
          const candidates = (body.candidates || []).map((c, idx) => ({
            ...c,
            applying: false,
            __idx: idx,
          }));
          this.refreshPosterFileState(videoPath, (current) => {
            if (current.searchRequestId !== requestId) {
              return current;
            }

            return {
              ...current,
              candidates,
              statusLine: candidates.length > 0 ? `找到 ${candidates.length} 张图片` : "未找到候选海报",
              statusType: candidates.length > 0 ? "ok" : "warning",
            };
          });
        } catch (error) {
          this.refreshPosterFileState(videoPath, (current) => {
            if (current.searchRequestId !== requestId) {
              return current;
            }

            return {
              ...current,
              statusLine: error.message,
              statusType: "error",
            };
          });
        } finally {
          this.refreshPosterFileState(videoPath, (current) => {
            if (current.searchRequestId !== requestId) {
              return current;
            }

            return {
              ...current,
              searching: false,
            };
          });
        }
      },

      async applyPoster(file, candidate) {
        const common = this.getCommonOptions();

        if (file.applying || candidate.applying) {
          return;
        }

        candidate.applying = true;
        file.applying = true;
        file.statusLine = "应用中...";
        file.statusType = "";

        const payload = {
          video_path: file.video_path,
          image_url: candidate.image_url,
          poster_style: common.poster_style,
          timeout: Number(this.form.timeout) || 20,
          proxy: this.form.proxy || null,
          ai_provider: this.form.ai_provider,
          ai_api_key: this.form.ai_api_key || null,
          ai_model: this.form.ai_model || null,
          ai_base_url: this.form.ai_base_url || null,
        };

        try {
          const response = await fetch("/api/poster/apply", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });

          if (!response.ok) {
            const error = await this.parseError(response);
            file.statusLine = error;
            file.statusType = "error";
            return;
          }

          const body = await response.json();
          file.poster_exists = body.poster_exists;
          file.poster_path = body.poster_path;
          file.statusLine = "应用成功";
          file.statusType = "ok";
          this.closeCandidateDialog();

          if (body.nfo_synced) {
            file.nfo_exists = true;
          }

          this.syncCurrentPage();
        } catch (error) {
          file.statusLine = error.message;
          file.statusType = "error";
        } finally {
          candidate.applying = false;
          file.applying = false;
        }
      },
    },

    mounted() {
      this.loadPreferences();
      this.startPolling(0);
      this.ensurePosterFilesLoaded();
    },

    beforeUnmount() {
      this.pollSession += 1;
      this.stopPolling();
    },
  });

  app.mount("#app");
})();
