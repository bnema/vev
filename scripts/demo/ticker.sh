#!/usr/bin/env bash
# Prints a visibly advancing counter; proves the session kept running
# while detached. Narrow-safe: one short line per second.
i=0
while true; do
  printf '\033[38;5;213mtick\033[0m %d\n' "$i"
  i=$((i + 1))
  sleep 1
done
