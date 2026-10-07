# Visual remote resilience acceptance

`make remote-resilience-visual` runs a disposable remote session in a real foot terminal under NeferWL headless. Physical keys enter through the compositor. PNG differences and passive vev captures jointly verify local picker response within two seconds. A stopped client is the negative control.

Prerequisites: Go 1.27, rootless Docker, Python 3, ssh-keygen, foot, and a GPU supported by NeferWL. Set `NEFERWL_SOURCE` to the compositor checkout or `NEFERWL_BIN` to its built binary. No desktop Wayland socket, user credentials or host network configuration is shared with containers.

```sh
make remote-resilience-visual
VEV_RESILIENCE_TRANSPORT=ssh make remote-resilience-visual
VEV_RESILIENCE_DEGRADED=1 make remote-resilience-visual
VEV_RESILIENCE_TRANSPORT=ssh VEV_RESILIENCE_DEGRADED=1 make remote-resilience-visual
VEV_RESILIENCE_LINK=udp make remote-resilience-visual
VEV_RESILIENCE_LINK=tcp make remote-resilience-visual
VEV_RESILIENCE_LONG=1 make remote-resilience-visual
VEV_RESILIENCE_LONG=1 VEV_RESILIENCE_RESUME=1 make remote-resilience-visual
VEV_RESILIENCE_LONG=1 VEV_RESILIENCE_CANCEL=Escape make remote-resilience-visual
```

QUIC is routed through the existing bounded seeded UDP simulator. A fixture-only SSH wrapper preserves authentication fields and rewrites the dynamic bootstrap port to its relay. SSH runs through a TCP relay. The degraded profile applies 150 ms delay, up to 50 ms jitter and 5% datagram loss to UDP; TCP waits 150 ms per forwarded read and is paced to 64 KiB/s, without dropping stream bytes; many small reads therefore add up to more than 150 ms. Both transports can be blacked out together without privileges.

The short scenario opens the client picker while connected, edits it during blackout, restores the route and checks the named session identity and recovery output. Opening the daemon-rendered palette during blackout is not a local responsiveness guarantee. The optional long scenario asserts actual Connecting state and Ctrl-C or Esc cancellation. It cannot tell backoff from a blocked stream open; fake-clock client tests cover cancellation during stream opening. It fails if cancellation cannot reach the resume owner.

Artifacts are printed at exit: captures, pixel counts, observation contexts, relay counters and fixture logs. Containers, image, network and temporary credentials are removed. Unsupported rendering and failed assertions exit nonzero.

This is not a mobile-network emulator: TCP impairments model stream backpressure, not packet retransmissions; IP migration, CGNAT and exactly-once replay are not validated. `VEV_RESILIENCE_LINK=udp|tcp` isolates short asymmetric blackouts. `VEV_RESILIENCE_RESUME=1` restores a long outage and checks two held fixture commands in order. Detach timing is covered by fake-clock client tests, not this visual entrypoint.
