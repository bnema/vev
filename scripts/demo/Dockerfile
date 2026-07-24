# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/vev .

FROM archlinux:latest
# The base image ships a pacman.conf that skips extracting man/info pages
# (NoExtract) to stay small; the demo wants a real `man ls`, so drop that
# rule and reinstall coreutils to pull its man page onto disk.
RUN pacman -Sy --noconfirm archlinux-keyring \
    && sed -i '/^NoExtract.*usr\/share\/man/d' /etc/pacman.conf \
    && pacman -Syu --noconfirm \
      vhs ttyd ffmpeg chromium \
      btop fastfetch gifsicle \
      eza neovim man-db man-pages coreutils \
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
ENV EZA_ICONS_AUTO=1
ENTRYPOINT ["vhs"]
