# syntax=docker/dockerfile:1
FROM archlinux:latest
RUN --mount=type=cache,target=/var/cache/pacman/pkg,sharing=locked \
    pacman -Sy --noconfirm archlinux-keyring \
    && sed -i '/^NoExtract.*usr\/share\/man/d' /etc/pacman.conf \
    && pacman -Syu --noconfirm \
      vhs ttyd ffmpeg chromium \
      htop fastfetch gifsicle imagemagick \
      eza neovim man-db man-pages coreutils \
      ncurses openssh util-linux \
      ttf-jetbrains-mono-nerd
RUN useradd -m -u 1000 demo
# P8.2 uses the exact worktree binary built by sandbox-run.sh, never a
# second in-image build that could diverge from the artifact under test.
COPY build/vev /usr/local/bin/vev
COPY scripts/demo/ticker.sh /usr/local/bin/ticker
COPY scripts/demo/demo-shell.sh /usr/local/bin/demo-shell
RUN chmod +x /usr/local/bin/ticker /usr/local/bin/demo-shell \
    && install -d -o demo -g demo -m 0700 /home/demo/.ssh \
    && install -d -m 0755 /run/sshd \
    && ssh-keygen -A \
    && ssh-keygen -q -t ed25519 -N '' -C vev-demo -f /home/demo/.ssh/id_ed25519 \
    && cp /home/demo/.ssh/id_ed25519.pub /home/demo/.ssh/authorized_keys \
    && { printf 'remote-a,remote-b '; cut -d' ' -f1-2 /etc/ssh/ssh_host_ed25519_key.pub; } > /home/demo/.ssh/known_hosts \
    && printf '%s\n' \
      'Host remote-a remote-b' \
      '    User demo' \
      '    IdentityFile ~/.ssh/id_ed25519' \
      '    IdentitiesOnly yes' \
      '    StrictHostKeyChecking yes' \
      '    UserKnownHostsFile ~/.ssh/known_hosts' \
      '    LogLevel ERROR' \
      > /home/demo/.ssh/config \
    && chmod 0600 /home/demo/.ssh/id_ed25519 /home/demo/.ssh/authorized_keys /home/demo/.ssh/known_hosts /home/demo/.ssh/config \
    && chown -R demo:demo /home/demo/.ssh
USER demo
WORKDIR /home/demo
ENV VHS_NO_SANDBOX=true
ENV COLORTERM=truecolor
ENV EZA_ICONS_AUTO=1
ENTRYPOINT ["vhs"]
