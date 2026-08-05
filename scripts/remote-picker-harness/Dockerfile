ARG BASE_IMAGE=ubuntu:24.04
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive \
    XDG_STATE_HOME=/tmp/vev-state \
    XDG_RUNTIME_DIR=/tmp/vev-runtime \
    PATH=/home/test/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    TERM=xterm-256color \
    COLORTERM=truecolor

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssh-client openssh-server \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash test \
    && usermod --password x test \
    && install -d -m 0755 /run/sshd /tmp/vev-state /tmp/vev-runtime /home/test/bin \
    && chmod 0777 /tmp/vev-state /tmp/vev-runtime \
    && install -d -m 0700 -o test -g test /home/test/.ssh

COPY vev /usr/local/bin/vev.real
COPY harness /usr/local/bin/remote-picker-harness
COPY vev-wrapper /usr/local/bin/vev
COPY id_ed25519.pub /home/test/.ssh/authorized_keys

RUN chown test:test /home/test/.ssh/authorized_keys \
    && chmod 0600 /home/test/.ssh/authorized_keys \
    && chmod 0755 /usr/local/bin/vev /usr/local/bin/vev.real /usr/local/bin/remote-picker-harness \
    && printf '%s\n' \
        'Host *' \
        '  IdentityFile /home/test/.ssh/id_ed25519' \
        '  StrictHostKeyChecking no' \
        '  UserKnownHostsFile /dev/null' \
        '  LogLevel ERROR' \
        > /home/test/.ssh/config \
    && chown test:test /home/test/.ssh/config \
    && chmod 0600 /home/test/.ssh/config \
    && printf '%s\n' \
        'PermitRootLogin no' \
        'PasswordAuthentication no' \
        'KbdInteractiveAuthentication no' \
        'PubkeyAuthentication yes' \
        'UsePAM no' \
        'AllowUsers test' \
        'PrintMotd no' \
        > /etc/ssh/sshd_config.d/vev-harness.conf \
    && ssh-keygen -A

CMD ["/usr/sbin/sshd", "-D", "-e"]
