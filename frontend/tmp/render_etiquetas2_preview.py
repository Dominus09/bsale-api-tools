from PIL import Image, ImageDraw, ImageFont
from pathlib import Path
import urllib.request
import io

OUT = Path(__file__).with_name("etiquetas2-mockup-ppum-preview.png")
OUT.parent.mkdir(parents=True, exist_ok=True)

PX_PER_MM = 12
LW, LH = 100 * PX_PER_MM, 40 * PX_PER_MM
GAP = 28
COLS, ROWS = 2, 2
W = COLS * LW + (COLS + 1) * GAP
H = ROWS * LH + (ROWS + 1) * GAP + 48

img = Image.new("RGB", (W, H), (245, 246, 248))
draw = ImageDraw.Draw(img)


def font(size, bold=False):
    candidates = [
        r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
        r"C:\Windows\Fonts\segoeuib.ttf" if bold else r"C:\Windows\Fonts\segoeui.ttf",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()


f_cat = font(18)
f_name = font(34, True)
f_var = font(20)
f_lab = font(16)
f_price_n = font(48, True)
f_price_s = font(56, True)
f_ppum = font(18)
f_code = font(18)
f_pill = font(15, True)
f_title = font(22, True)

logo = None
try:
    url = (
        "https://hebbkx1anhila5yf.public.blob.vercel-storage.com/"
        "GRUPO%20QUILLOTANA%20PS-fK4da0sPbUwnmEpeEVmmumWdj977f0.png"
    )
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": "preview"}), timeout=20
    ) as r:
        logo = Image.open(io.BytesIO(r.read())).convert("RGBA")
except Exception as e:
    print("logo fail", e)

SOCIO = (0, 90, 168)
SOFT = (244, 249, 253)

samples = [
    dict(
        cat="ABARROTES",
        name="ACEITE BONANZA",
        var="VEGETAL 900 CC (SEC 12)",
        n=2290,
        s=1825,
        code="7808743600555",
        pn="$2.544 por litro",
        ps="$2.028 por litro",
    ),
    dict(
        cat="BEBIDAS",
        name="CRISTAL RETORNABLE",
        var="1.2 LT (SEC 12)",
        n=1290,
        s=1090,
        code="7802910001234",
        pn="$1.075 por litro",
        ps="$908 por litro",
    ),
    dict(
        cat="ABARROTES",
        name="AZÚCAR IANSA",
        var="1 KG",
        n=1190,
        s=990,
        code="7802920009988",
        pn="$1.190 por kg",
        ps="$990 por kg",
    ),
    dict(
        cat="BEBIDAS",
        name="COCA-COLA SIN AZÚCAR ZERO SUGAR RETORNABLE",
        var="2.5 LT (SEC 6)",
        n=2590,
        s=2190,
        code="7801610755011",
        pn="$1.036 por litro",
        ps="$876 por litro",
    ),
]


def money(v):
    return f"${v:,.0f}".replace(",", ".")


draw.text(
    (GAP, 14),
    "Etiquetas Socios V2 · Socio Estándar 10×4 cm · PPUM · Cód. textual",
    fill=(40, 40, 40),
    font=f_title,
)

for i, s in enumerate(samples):
    col, row = i % COLS, i // COLS
    x = GAP + col * (LW + GAP)
    y = 48 + GAP + row * (LH + GAP)

    draw.rounded_rectangle(
        [x, y, x + LW, y + LH],
        radius=8,
        fill=(255, 255, 255),
        outline=(222, 222, 222),
        width=2,
    )

    pad = 36
    logo_w = 210
    logo_h = 100
    if logo is not None:
        aspect = logo.width / logo.height
        logo_h = min(int(logo_w / aspect), 120)
        logo_w = int(logo_h * aspect)
        logo_r = logo.resize((logo_w, logo_h), Image.Resampling.LANCZOS)
        img.paste(logo_r, (x + pad, y + 22), logo_r)
    else:
        draw.rectangle([x + pad, y + 22, x + pad + 160, y + 100], fill=(200, 30, 40))

    tx = x + pad + logo_w + 28
    tw = x + LW - pad - tx
    cy = y + 28
    draw.text((tx, cy), s["cat"], fill=(140, 140, 140), font=f_cat)
    cy += 26
    words = s["name"].split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        if draw.textlength(test, font=f_name) <= tw:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    lines = lines[:2]
    if len(lines) == 2 and len(" ".join(lines).split()) < len(words):
        while draw.textlength(lines[1] + "…", font=f_name) > tw and len(lines[1]) > 4:
            lines[1] = lines[1][:-1]
        lines[1] = lines[1].rstrip() + "…"
    for ln in lines:
        draw.text((tx, cy), ln, fill=(18, 18, 18), font=f_name)
        cy += 38
    draw.text((tx, cy + 2), s["var"], fill=(90, 90, 90), font=f_var)

    band_top = y + int(LH * 0.33)
    band_h = int(LH * 0.50)
    mid = x + int(LW * 0.40)
    draw.rounded_rectangle(
        [mid + 8, band_top + 10, x + LW - pad + 8, band_top + band_h - 10],
        radius=12,
        fill=SOFT,
    )

    nx = x + pad
    draw.text((nx, band_top + 28), "PRECIO NORMAL", fill=(120, 120, 120), font=f_lab)
    draw.text((nx, band_top + 58), money(s["n"]), fill=(18, 18, 18), font=f_price_n)
    draw.text((nx, band_top + 118), s["pn"], fill=(110, 110, 110), font=f_ppum)

    scx = (mid + x + LW - pad) // 2
    pill = "SOCIO QUILLOTANA"
    pw = int(draw.textlength(pill, font=f_pill)) + 36
    ph = 28
    px0 = scx - pw // 2
    py0 = band_top + 22
    draw.rounded_rectangle([px0, py0, px0 + pw, py0 + ph], radius=ph // 2, fill=SOCIO)
    draw.text(
        (scx - draw.textlength(pill, font=f_pill) / 2, py0 + 5),
        pill,
        fill=(255, 255, 255),
        font=f_pill,
    )
    sp = money(s["s"])
    draw.text(
        (scx - draw.textlength(sp, font=f_price_s) / 2, py0 + 40),
        sp,
        fill=SOCIO,
        font=f_price_s,
    )
    draw.text(
        (scx - draw.textlength(s["ps"], font=f_ppum) / 2, py0 + 105),
        s["ps"],
        fill=(70, 110, 150),
        font=f_ppum,
    )

    code = f"Cód: {s['code']}"
    draw.text(
        (x + LW - pad - draw.textlength(code, font=f_code), y + LH - 34),
        code,
        fill=(100, 100, 100),
        font=f_code,
    )

img.save(OUT, "PNG")
print("saved", OUT, img.size)
