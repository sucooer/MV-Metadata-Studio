# MV Emby Scraper (Docker + Web UI)

一个用于批量生成 Emby 可识别 MV 刮削信息的应用，支持：

- CLI 批处理
- Web 控制台手动触发任务 + 实时日志
- 每个 MV 单独点击搜索海报并应用

它会：

- 从视频文件名或文件夹名提取 `歌手` + `歌曲名`
- 自动搜索网络信息（YouTube + iTunes 公共接口）
- 为每个视频生成：
  - `xxx.nfo`
  - `xxx-poster.jpg`（或 `poster.jpg`）

## 支持的命名示例

- `Adele - Hello.mp4`
- `[Adele] Hello.mkv`
- `Hello by Adele.mp4`
- 文件夹名兜底：`Adele - Hello/clip.mp4`

## Web 控制台（推荐）

### 本地启动

```bash
pip install -r requirements.txt
python -m mv_scraper.web --host 0.0.0.0 --port 7860
```

打开浏览器：

- `http://127.0.0.1:7860`

页面默认扫描路径已预填：

- `C:\Users\anyaer\Videos\strm\mv`

### 主要操作

1. 在“任务参数”里确认扫描路径、海报命名方式。
2. 如需专业化 `plot` 文案，可填写 AI 服务商 / API Key / 模型（可选）。
   - 内置支持：OpenAI、OpenRouter、DeepSeek、SiliconFlow、自定义 OpenAI 兼容网关
3. 如需代理，在“代理”输入：
   - `http://127.0.0.1:7890`
   - 或 `socks5://127.0.0.1:1080`
   - 可点击“检测代理”查看可用性和延迟（iTunes / YouTube / lgych）
4. 点击“读取MV列表”。
5. 在“Poster 工作台”中对每个 MV 点击“搜索海报”。
6. 从候选图里点击“应用为海报”。
7. 需要批量刮削时，再点击“开始批处理”。

说明：

- 手动应用海报时会同步生成/补齐该 MV 的 `.nfo` 信息。
- 如果同目录已有 `.nfo`，会自动同步更新 `thumb` 字段。

## Docker 启动 Web

1. 构建镜像：

```bash
docker build -t mv-emby-scraper .
```

2. 运行容器并映射媒体目录（示例容器端路径 `/data`）：

```bash
docker run --rm -p 7860:7860 \
  -v /path/to/your/mv:/data \
  mv-emby-scraper
```

打开浏览器：

- `http://127.0.0.1:7860`

## CLI 运行

本地：

```bash
python -m mv_scraper.cli "./your-mv-folder" --recursive --verbose
```

带代理：

```bash
python -m mv_scraper.cli "./your-mv-folder" --proxy http://127.0.0.1:7890
```

Docker（覆盖默认 CMD，切到 CLI）：

```bash
docker run --rm \
  -v /path/to/your/mv:/data \
  mv-emby-scraper mv_scraper.cli /data --recursive --verbose
```

仅预览解析结果（不写文件）：

```bash
python -m mv_scraper.cli "./your-mv-folder" --dry-run
```

## 常用参数

- `--recursive` / `--no-recursive`：是否递归扫描目录
- `--default-artist "周杰伦"`：当只能识别歌曲名时，使用默认歌手
- `--poster-style basename|folder`：海报命名规则
  - `basename`（默认）：`video-poster.jpg`
  - `folder`：`poster.jpg`
- `--overwrite`：覆盖已存在的 NFO/海报
- `--timeout 20`：网络请求超时秒数
- `--proxy <url>`：网络代理 URL
- `--ai-provider <name>`：AI 服务商（`openai|openrouter|deepseek|siliconflow|custom`）
- `--ai-api-key <key>`：可选，AI Key（兼容旧参数 `--openai-api-key`）
- `--ai-model <model>`：可选，AI 模型（兼容旧参数 `--openai-model`）
- `--ai-base-url <url>`：可选，覆盖默认 API 地址（用于自定义兼容网关）
- `--dry-run`：只解析，不写文件

## 输出说明

默认每个视频会产出：

- `video.nfo`
- `video-poster.jpg`

NFO 使用 `<musicvideo>` 结构，包含常见字段（title、artist、album、plot、premiered、year、studio、thumb、uniqueid 等），可被 Emby 读取。
若配置了 AI 参数，`<plot>` 会优先由 AI 结合歌曲信息生成；未配置时会使用清洗后的模板文案，避免直接写入原始 YouTube 简介。
同时会在可用时写入 `rating` / `userrating` / `votes`（基于可获取的公开视频热度估算）。
