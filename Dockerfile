FROM golang:1.23-alpine AS build

WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY mv_scraper ./mv_scraper

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/mv-metadata-studio ./cmd/server

FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=build /out/mv-metadata-studio /app/mv-metadata-studio
COPY mv_scraper /app/mv_scraper

ENTRYPOINT ["/app/mv-metadata-studio"]
CMD ["--host", "0.0.0.0", "--port", "7860"]
