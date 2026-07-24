# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/vev .

FROM archlinux:latest
RUN pacman -Sy --noconfirm archlinux-keyring \
    && pacman -Syu --noconfirm \
      vhs ttyd ffmpeg chromium \
      btop fastfetch gifsicle \
      nodejs npm ncurses \
      ttf-jetbrains-mono-nerd \
    && pacman -Scc --noconfirm
RUN npm install -g --allow-scripts=@anthropic-ai/claude-code @anthropic-ai/claude-code
RUN setcap -r /usr/sbin/btop
RUN useradd -m -u 1000 demo
COPY --from=build /out/vev /usr/local/bin/vev
COPY scripts/demo/ticker.sh /usr/local/bin/ticker
RUN chmod +x /usr/local/bin/ticker
USER demo
WORKDIR /home/demo
ENV VHS_NO_SANDBOX=true
ENV COLORTERM=truecolor
ENTRYPOINT ["vhs"]
