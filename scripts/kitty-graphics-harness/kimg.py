#!/usr/bin/env python3
"""Emit Kitty graphics like agent TUIs do (a=T,f=100,C=1 with only c= or r=)."""
import base64, io, os, sys
from PIL import Image, ImageDraw

def png(w, h, color):
    im = Image.new("RGB", (w, h), color)
    d = ImageDraw.Draw(im)
    d.rectangle([0, 0, w - 1, h - 1], outline=(255, 255, 255), width=max(2, w // 60))
    d.line([0, 0, w - 1, h - 1], fill=(255, 255, 255), width=max(2, w // 80))
    d.line([0, h - 1, w - 1, 0], fill=(255, 255, 255), width=max(2, w // 80))
    b = io.BytesIO(); im.save(b, "PNG"); return b.getvalue()

def emit(data, ctl):
    b64 = base64.standard_b64encode(data).decode()
    out = []
    for i in range(0, len(b64), 4096):
        more = 1 if i + 4096 < len(b64) else 0
        head = f"a=T,f=100,C=1,{ctl},m={more}" if i == 0 else f"m={more}"
        out.append(f"\x1b_G{head};{b64[i:i+4096]}\x1b\\")
    sys.stdout.write("".join(out))

cases = [
    ("small 200x100 c=20", png(200, 100, (200, 40, 40)), "c=20", 5),
    ("wide 1600x400 c=40", png(1600, 400, (40, 160, 40)), "c=40", 5),
    ("big 1600x1200 c=24", png(1600, 1200, (40, 40, 200)), "c=24", 9),
    ("tall 300x1200 r=8", png(300, 1200, (180, 140, 20)), "r=8", 8),
]
for n in range(int(os.environ.get("PRE","0"))):
    sys.stdout.write("\r\n")
sel = sys.argv[1:] or [str(i) for i in range(len(cases))]
for i in sel:
    name, data, ctl, rows = cases[int(i)]
    sys.stdout.write(f"--- {name}\r\n")
    emit(data, ctl)
    sys.stdout.write("\r\n" * rows)
for n in range(int(os.environ.get("PAD","0"))):
    sys.stdout.write(f"pad line {n}\r\n")
sys.stdout.write("--- end\r\n")
sys.stdout.flush()
