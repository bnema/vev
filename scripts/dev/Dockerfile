# syntax=docker/dockerfile:1
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/vev .

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends bash ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 --shell /bin/bash vev
COPY --from=build /out/vev /usr/local/bin/vev
USER vev
WORKDIR /home/vev
ENV HOME=/home/vev \
    SHELL=/bin/bash \
    TERM=xterm-256color \
    COLORTERM=truecolor \
    XDG_STATE_HOME=/home/vev/.local/state
ENTRYPOINT ["/usr/local/bin/vev"]
