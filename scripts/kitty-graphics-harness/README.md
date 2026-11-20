# Kitty graphics harness

Renders Kitty images inside vev in a headless NeferWL session with a real kitty window, then saves PNG screenshots. `kimg.py` sends images the way agent TUIs do (`a=T,f=100,C=1` with only `c=` or `r=`).

```sh
go build -o /tmp/vev .
scripts/kitty-graphics-harness/run.sh fix /tmp/vev 0 1 2 3      # all cases inside vev
scripts/kitty-graphics-harness/run.sh base none 0 1 2 3         # same cases in plain kitty
PRE=25 scripts/kitty-graphics-harness/run.sh bottom /tmp/vev 2  # big image on the last rows
PAD=20 WHEEL=3 scripts/kitty-graphics-harness/run.sh scroll /tmp/vev 0 2 3  # wheel into copy mode
```

Cases: `0` small, `1` wide (`c=40`), `2` big (`c=24`), `3` tall (`r=8`).

Each run writes `1-after.png`, `2-scrolled.png` (after mouse-wheel up) and `3-back.png` to `$OUTDIR/out/<label>/`. vev runs with private `XDG_*` directories and is stopped at the end.
