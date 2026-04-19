# MV Emby Scraper

批量生成 Emby 可识别的 MV 刮削信息（海报 + NFO）。

## 快速开始

```bash
pip install -r requirements.txt
python -m mv_scraper.web --host 0.0.0.0 --port 7860
```

打开 http://127.0.0.1:7860

## 功能

- 批量刮削 MV 海报和 NFO 信息
- Web UI 手动搜索/应用海报
- 支持 AI 生成 plot 文案（OpenAI / DeepSeek / OpenRouter 等）

## 支持的命名格式

- `Adele - Hello.mp4`
- `[Adele] Hello.mkv`
- `Hello by Adele.mp4`

## Docker

```bash
docker build -t mv-emby-scraper .
docker run --rm -p 7860:7860 -v /path/to/mv:/data mv-emby-scraper
```

## CLI

```bash
python -m mv_scraper.cli "./mv-folder" --recursive
```
