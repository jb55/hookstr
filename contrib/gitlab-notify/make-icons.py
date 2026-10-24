#!/usr/bin/env python3
"""Draw gitlab-notify's signal icons into icons/*.png. Stdlib only.

Each icon is a coloured disc with a white glyph, rasterized from signed
distance functions (one sample per pixel, antialiased by distance) and
written as RGBA PNG. Rerun after changing a shape or colour.
"""

import math
import os
import struct
import zlib

SIZE = 256
WHITE = (255, 255, 255)


def png(path, w, h, rgba):
    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))
    raw = b"".join(b"\x00" + bytes(rgba[y * w * 4:(y + 1) * w * 4]) for y in range(h))
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)))
        f.write(chunk(b"IDAT", zlib.compress(raw, 9)))
        f.write(chunk(b"IEND", b""))


# Shapes work in unit coordinates: the disc spans [-1, 1].

def circle(cx, cy, r):
    return lambda x, y: math.hypot(x - cx, y - cy) - r


def ellipse(cx, cy, rx, ry):
    # Approximate distance; fine at these sizes.
    return lambda x, y: (math.hypot((x - cx) / rx, (y - cy) / ry) - 1) * min(rx, ry)


def segment(ax, ay, bx, by, t):
    def d(x, y):
        px, py, vx, vy = x - ax, y - ay, bx - ax, by - ay
        h = max(0.0, min(1.0, (px * vx + py * vy) / (vx * vx + vy * vy)))
        return math.hypot(px - vx * h, py - vy * h) - t
    return d


def polyline(points, t):
    segs = [segment(*points[i], *points[i + 1], t) for i in range(len(points) - 1)]
    return lambda x, y: min(s(x, y) for s in segs)


def rounded_rect(cx, cy, hw, hh, r):
    def d(x, y):
        qx, qy = abs(x - cx) - hw + r, abs(y - cy) - hh + r
        return math.hypot(max(qx, 0), max(qy, 0)) + min(max(qx, qy), 0) - r
    return d


def triangle(a, b, c):
    def d(x, y):
        def edge(p, q):
            return (q[0] - p[0]) * (y - p[1]) - (q[1] - p[1]) * (x - p[0])
        e = (edge(a, b), edge(b, c), edge(c, a))
        inside = all(v >= 0 for v in e) or all(v <= 0 for v in e)
        dist = min(segment(*a, *b, 0)(x, y), segment(*b, *c, 0)(x, y), segment(*c, *a, 0)(x, y))
        return -dist if inside else dist
    return d


def union(*fs):
    return lambda x, y: min(f(x, y) for f in fs)


T = 0.11  # glyph stroke half-width

ICONS = {
    # kind: (disc colour, [(sdf, colour), ...] painted in order)
    "pipeline_passed": ((34, 160, 90), [
        (polyline([(-0.42, 0.02), (-0.12, 0.32), (0.45, -0.3)], T), WHITE)]),
    "pipeline_failed": ((214, 54, 56), [
        (union(segment(-0.36, -0.36, 0.36, 0.36, T), segment(-0.36, 0.36, 0.36, -0.36, T)), WHITE)]),
    "approved": ((37, 110, 210), [
        (polyline([(-0.4, -0.05), (-0.12, 0.23), (0.42, -0.33)], T), WHITE),
        (segment(-0.38, 0.47, 0.38, 0.47, 0.07), WHITE)]),
    "changes_requested": ((230, 126, 20), [
        (segment(0, -0.48, 0, 0.12, T + 0.01), WHITE),
        (circle(0, 0.42, 0.13), WHITE)]),
    "conflict": ((196, 82, 20), [
        (polyline([(0.12, -0.6), (-0.28, 0.06), (0.28, -0.06), (-0.12, 0.6)], T), WHITE)]),
    "new_mr": ((84, 72, 200), [
        (union(segment(0, -0.42, 0, 0.42, T), segment(-0.42, 0, 0.42, 0, T)), WHITE)]),
    "merged": ((128, 64, 190), [
        (union(segment(-0.3, -0.45, -0.3, 0.45, 0.09),
               polyline([(0.3, -0.45), (0.3, -0.1), (-0.3, 0.3)], 0.09)), WHITE),
        (circle(-0.3, -0.45, 0.15), WHITE), (circle(0.3, -0.45, 0.15), WHITE),
        (circle(-0.3, 0.45, 0.15), WHITE)]),
    "closed": ((110, 116, 128), [
        (segment(-0.42, 0, 0.42, 0, T), WHITE)]),
    "review": ((20, 140, 150), [
        (lambda x, y: abs(ellipse(0, 0, 0.58, 0.34)(x, y)) - 0.075, WHITE),
        (circle(0, 0, 0.17), WHITE)]),
    "comments": ((70, 96, 140), [
        (union(rounded_rect(0, -0.1, 0.55, 0.36, 0.16),
               triangle((-0.3, 0.2), (-0.05, 0.2), (-0.38, 0.55))), WHITE),
        (union(circle(-0.25, -0.1, 0.075), circle(0, -0.1, 0.075), circle(0.25, -0.1, 0.075)),
         None)]),  # None: punch through to the disc colour
    "push": ((60, 130, 100), [
        (union(segment(0, -0.45, 0, 0.45, T),
               polyline([(-0.32, -0.12), (0, -0.45), (0.32, -0.12)], T)), WHITE)]),
}


def cover(d, px):
    """Coverage of a pixel from a signed distance in unit space."""
    return max(0.0, min(1.0, 0.5 - d / px))


# The helper app's bundle icon (macos/build.sh turns it into an .icns): a
# merge glyph on macOS's rounded-square icon plate (824 of 1024 px), with
# an unread dot.
APP_SIZE = 1024
APP = ((84, 72, 200), [
    (union(segment(-0.3, -0.4, -0.3, 0.4, 0.07),
           polyline([(0.25, 0.4), (0.25, 0.02), (-0.3, -0.36)], 0.07)), WHITE),
    (circle(-0.3, -0.4, 0.12), WHITE), (circle(-0.3, 0.4, 0.12), WHITE),
    (circle(0.25, 0.4, 0.12), WHITE),
    (circle(0.4, -0.4, 0.17), (240, 120, 40))])
APP_PLATE = rounded_rect(0, 0, 0.805, 0.805, 0.36)


def render(disc, layers, size=SIZE, bg=circle(0, 0, 0.94)):
    px = 2.0 / size
    out = bytearray(size * size * 4)
    for j in range(size):
        y = (j + 0.5) * px - 1
        for i in range(size):
            x = (i + 0.5) * px - 1
            a = cover(bg(x, y), px)
            if a <= 0:
                continue
            r, g, b = disc
            for sdf, colour in layers:
                c = cover(sdf(x, y), px)
                if c > 0:
                    col = colour or disc
                    r, g, b = (r + (col[0] - r) * c, g + (col[1] - g) * c, b + (col[2] - b) * c)
            k = (j * size + i) * 4
            out[k:k + 4] = bytes((round(r), round(g), round(b), round(a * 255)))
    return out


def main():
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icons")
    os.makedirs(here, exist_ok=True)
    for kind, (disc, layers) in ICONS.items():
        png(os.path.join(here, kind + ".png"), SIZE, SIZE, render(disc, layers))
        print(kind)
    png(os.path.join(here, "app.png"), APP_SIZE, APP_SIZE, render(*APP, APP_SIZE, APP_PLATE))
    print("app")


if __name__ == "__main__":
    main()
