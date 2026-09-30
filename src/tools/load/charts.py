"""포화 곡선을 PNG 로 그린다. Pillow 가 있을 때만 동작하고, 없으면 조용히 건너뛴다.

matplotlib 같은 큰 의존성을 들이지 않고 Pillow(스크린샷 렌더에 이미 쓰는 것)로 축·격자·점·선을
직접 그린다. 폰트는 한글 라벨을 위해 Noto Sans CJK 를 찾고, 없으면 기본 폰트로 떨어진다.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Sequence

#: 라벨용 폰트 후보(한글 지원). 없으면 Pillow 기본 폰트를 쓴다.
_FONT_CANDIDATES = [
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 6),   # Mono CJK KR
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 0),
    ("/usr/share/fonts/truetype/noto/NotoSansMono-Regular.ttf", 0),
    ("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf", 0),
]

BG = (255, 255, 255)
AXIS = (60, 60, 68)
GRID = (224, 224, 230)
LINE = (70, 130, 200)
DOT = (70, 130, 200)
KNEE = (214, 90, 70)
TEXT = (40, 40, 48)
DIM = (120, 120, 132)


def available() -> bool:
    """Pillow 를 쓸 수 있는지."""
    try:
        import PIL  # noqa: F401
        return True
    except ImportError:
        return False


def _font(size: int):
    from PIL import ImageFont
    for path, idx in _FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size, index=idx)
            except Exception:
                continue
    return ImageFont.load_default()


def render_saturation_png(levels: Sequence[Dict[str, Any]], knee: Optional[Dict[str, Any]],
                          path: str, field: str = "complete_tps",
                          title: str = "완료 TPS vs VU") -> bool:
    """포화 곡선 PNG 를 ``path`` 에 저장한다. Pillow 가 없거나 점이 부족하면 False 를 돌려준다."""
    if not available():
        return False
    pts = [lv for lv in levels if lv["seconds"] > 0]
    if len(pts) < 2:
        return False
    from PIL import Image, ImageDraw

    W, H = 900, 500
    ml, mr, mt, mb = 90, 40, 60, 70   # 여백(좌/우/상/하)
    pw, ph = W - ml - mr, H - mt - mb
    vals = [lv[field] for lv in pts]
    vmax = max(vals) or 1.0
    ymax = vmax * 1.12
    n = len(pts)

    def px(i: int) -> float:
        return ml + (pw * (i + 0.5) / n)

    def py(v: float) -> float:
        return mt + ph * (1 - v / ymax)

    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    f_title = _font(24)
    f_lbl = _font(18)
    f_small = _font(15)

    d.text((ml, 18), title, font=f_title, fill=TEXT)

    # y 격자와 눈금(0 ~ vmax 를 4등분). 작은 값은 소수 한 자리로 뭉침을 막는다.
    yfmt = "{:.0f}" if vmax >= 10 else "{:.1f}"
    for k in range(5):
        v = vmax * k / 4
        y = py(v)
        d.line([(ml, y), (W - mr, y)], fill=GRID)
        d.text((ml - 12, y), yfmt.format(v), font=f_small, fill=DIM, anchor="rm")
    # 축
    d.line([(ml, mt), (ml, mt + ph)], fill=AXIS, width=2)
    d.line([(ml, mt + ph), (W - mr, mt + ph)], fill=AXIS, width=2)
    d.text((W - mr, mt + ph + 44), "VU", font=f_lbl, fill=TEXT, anchor="rm")

    knee_vus = knee["knee_vus"] if knee and knee.get("saturated") else None

    # 무릎점 세로 점선
    if knee_vus is not None:
        for i, lv in enumerate(pts):
            if lv["vus"] == knee_vus:
                x = px(i)
                y = mt
                while y < mt + ph:
                    d.line([(x, y), (x, min(y + 6, mt + ph))], fill=(230, 200, 195), width=1)
                    y += 12
                break

    # 선
    xy = [(px(i), py(v)) for i, v in enumerate(vals)]
    d.line(xy, fill=LINE, width=2)
    # 점과 x 라벨
    for i, (lv, v) in enumerate(zip(pts, vals)):
        x, y = px(i), py(v)
        is_knee = lv["vus"] == knee_vus
        r = 7 if is_knee else 5
        color = KNEE if is_knee else DOT
        if is_knee:
            d.polygon([(x, y - r - 1), (x + r + 1, y), (x, y + r + 1), (x - r - 1, y)], fill=color)
        else:
            d.ellipse([x - r, y - r, x + r, y + r], fill=color)
        d.text((x, y - r - 6), f"{v:.1f}", font=f_small, fill=TEXT, anchor="mb")
        d.text((x, mt + ph + 8), str(lv["vus"]), font=f_lbl, fill=TEXT, anchor="ma")

    # 무릎점 주석
    if knee is not None:
        note = (f"포화 VU ≈ {knee['knee_vus']} · {knee['knee_tps']:.1f} TPS"
                if knee["saturated"] else
                f"측정 범위에서 포화 안 됨(최고 VU {knee['peak_vus']} · {knee['peak_tps']:.1f} TPS)")
        d.text((ml + 6, mt + 4), note, font=f_lbl, fill=KNEE)

    _atomic_save(img, path)
    return True


def _atomic_save(img: Any, path: str) -> None:
    tmp = f"{path}.tmp"
    img.save(tmp, "PNG")
    os.replace(tmp, path)


def render_saturation_svg(levels: Sequence[Dict[str, Any]], knee: Optional[Dict[str, Any]],
                          field: str = "complete_tps") -> Optional[str]:
    """Pillow 없이도 쓸 수 있는 대체 경로로 SVG 문자열을 만든다(선택).

    에어갭에서도 브라우저로 열 수 있고 텍스트라 diff 가 되며, 의존성이 없다. PNG 가 필요 없을 때
    ``--chart out.svg`` 로 쓴다.
    """
    pts = [lv for lv in levels if lv["seconds"] > 0]
    if len(pts) < 2:
        return None
    W, H, ml, mr, mt, mb = 900, 500, 90, 40, 60, 70
    pw, ph = W - ml - mr, H - mt - mb
    vals = [lv[field] for lv in pts]
    vmax = max(vals) or 1.0
    ymax = vmax * 1.12
    n = len(pts)
    knee_vus = knee["knee_vus"] if knee and knee.get("saturated") else None

    def px(i):
        return ml + pw * (i + 0.5) / n

    def py(v):
        return mt + ph * (1 - v / ymax)

    e = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
         f'font-family="monospace" font-size="15">',
         f'<rect width="{W}" height="{H}" fill="white"/>']
    for k in range(5):
        v = vmax * k / 4
        y = py(v)
        e.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{W-mr}" y2="{y:.1f}" stroke="#e0e0e6"/>')
        e.append(f'<text x="{ml-10}" y="{y+4:.1f}" text-anchor="end" fill="#78788a">{v:.0f}</text>')
    e.append(f'<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{mt+ph}" stroke="#3c3c44" stroke-width="2"/>')
    e.append(f'<line x1="{ml}" y1="{mt+ph}" x2="{W-mr}" y2="{mt+ph}" stroke="#3c3c44" stroke-width="2"/>')
    pl = " ".join(f"{px(i):.1f},{py(v):.1f}" for i, v in enumerate(vals))
    e.append(f'<polyline points="{pl}" fill="none" stroke="#4682c8" stroke-width="2"/>')
    for i, (lv, v) in enumerate(zip(pts, vals)):
        x, y = px(i), py(v)
        if lv["vus"] == knee_vus:
            e.append(f'<polygon points="{x:.1f},{y-8:.1f} {x+8:.1f},{y:.1f} {x:.1f},{y+8:.1f} '
                     f'{x-8:.1f},{y:.1f}" fill="#d65a46"/>')
        else:
            e.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#4682c8"/>')
        e.append(f'<text x="{x:.1f}" y="{y-12:.1f}" text-anchor="middle" fill="#282830">{v:.1f}</text>')
        e.append(f'<text x="{x:.1f}" y="{mt+ph+22:.1f}" text-anchor="middle" fill="#282830">'
                 f'{lv["vus"]}</text>')
    e.append(f'<text x="{ml+6}" y="{mt+20}" fill="#d65a46">{title_note(knee)}</text>')
    e.append(f'<text x="{ml}" y="34" font-size="22" fill="#282830">{field} vs VU</text>')
    e.append('</svg>')
    return "\n".join(e)


def title_note(knee: Optional[Dict[str, Any]]) -> str:
    if knee is None:
        return ""
    if knee["saturated"]:
        return f"포화 VU ≈ {knee['knee_vus']} · {knee['knee_tps']:.1f} TPS"
    return f"측정 범위에서 포화 안 됨(최고 VU {knee['peak_vus']})"
