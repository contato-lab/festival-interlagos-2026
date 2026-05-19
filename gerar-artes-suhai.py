"""
Gerador de artes para Suhai Festival Interlagos 2026.
Cria PNG 1080x1080 (feed) e 1080x1920 (story/reels).
"""
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps, ImageEnhance, ImageChops
import os

ROOT = r"C:\Users\erice\Desktop\Festival Interlagos 2026\Dashboard\.claude\worktrees\upbeat-sammet-aa771b"
RAW = os.path.join(ROOT, "artistas-raw")
FONTS = os.path.join(ROOT, "fonts")
OUT = os.path.join(ROOT, "artes-suhai")
os.makedirs(OUT, exist_ok=True)

# Paleta Suhai Festival Interlagos
NEON = (197, 255, 77)       # verde lima neon - cor de marca
NEON_DARK = (152, 210, 30)
BLACK = (10, 10, 10)
WHITE = (255, 255, 255)
GRAY = (160, 160, 160)

# Fontes
F_DISPLAY = os.path.join(FONTS, "Anton-Regular.ttf")           # Hero/títulos massivos
F_BEBAS = os.path.join(FONTS, "BebasNeue-Regular.ttf")         # Subtítulos condensados
F_BOLD = r"C:\Windows\Fonts\arialbd.ttf"                       # Corpo bold
F_NARROW = r"C:\Windows\Fonts\ARIALNB.TTF"                     # Narrow bold

def font(path, size):
    return ImageFont.truetype(path, size)

# ------------- PREP IMAGENS DOS ARTISTAS -------------

def crop_matue_from_banner():
    """Corta o card do Matuê do banner MOTO_30-01.png (canto esquerdo)."""
    img = Image.open(os.path.join(RAW, "MOTO_30-01.png")).convert("RGBA")
    w, h = img.size  # 2346 x 1011
    # 3 cards iguais; Matuê é o primeiro à esquerda
    card_w = w // 3
    # tirar uns 10% das bordas
    pad = int(card_w * 0.05)
    crop = img.crop((pad, 0, card_w - pad, h))
    return crop

def remove_white_bg(img, threshold=235):
    """Remove fundo branco/claro substituindo por transparência."""
    img = img.convert("RGBA")
    data = img.getdata()
    new = []
    for r, g, b, a in data:
        # Pixel quase branco vira transparente
        if r > threshold and g > threshold and b > threshold:
            new.append((r, g, b, 0))
        else:
            new.append((r, g, b, a))
    img.putdata(new)
    return img

def make_portrait(src_path, target_w, target_h, crop_top_ratio=0.0):
    """Carrega foto, aplica crop centralizado vertical (estilo retrato), redimensiona."""
    img = Image.open(src_path).convert("RGB")
    w, h = img.size
    # Cropa pra aspecto desejado
    target_ratio = target_w / target_h
    src_ratio = w / h
    if src_ratio > target_ratio:
        # mais larga que alvo: corta laterais
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        img = img.crop((left, 0, left + new_w, h))
    else:
        # mais alta: corta de baixo, mantém topo (rosto)
        new_h = int(w / target_ratio)
        top = int(h * crop_top_ratio)
        img = img.crop((0, top, w, top + min(new_h, h - top)))
    img = img.resize((target_w, target_h), Image.LANCZOS)
    return img

def make_portrait_from_pil(img, target_w, target_h, crop_top_ratio=0.0):
    img = img.convert("RGB")
    w, h = img.size
    target_ratio = target_w / target_h
    src_ratio = w / h
    if src_ratio > target_ratio:
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        img = img.crop((left, 0, left + new_w, h))
    else:
        new_h = int(w / target_ratio)
        top = int(h * crop_top_ratio)
        img = img.crop((0, top, w, top + min(new_h, h - top)))
    return img.resize((target_w, target_h), Image.LANCZOS)

def rounded_mask(size, radius):
    """Cria máscara retângulo arredondado."""
    mask = Image.new("L", size, 0)
    ImageDraw.Draw(mask).rounded_rectangle([(0, 0), size], radius=radius, fill=255)
    return mask

def apply_gradient_overlay(img, start=(0, 0, 0, 0), end=(0, 0, 0, 220)):
    """Aplica gradiente preto de cima pra baixo na imagem (legibilidade do texto)."""
    w, h = img.size
    grad = Image.new("RGBA", (1, h), 0)
    for y in range(h):
        t = y / h
        r = int(start[0] * (1 - t) + end[0] * t)
        g = int(start[1] * (1 - t) + end[1] * t)
        b = int(start[2] * (1 - t) + end[2] * t)
        a = int(start[3] * (1 - t) + end[3] * t)
        grad.putpixel((0, y), (r, g, b, a))
    grad = grad.resize((w, h))
    out = img.convert("RGBA")
    out.alpha_composite(grad)
    return out

# ------------- HELPERS DESENHO -------------

def text_size(draw, text, fnt):
    bbox = draw.textbbox((0, 0), text, font=fnt)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]

def draw_text_centered(draw, text, fnt, y, color, canvas_w, letter_spacing=0):
    if letter_spacing == 0:
        w, _ = text_size(draw, text, fnt)
        draw.text(((canvas_w - w) // 2, y), text, font=fnt, fill=color)
    else:
        # Desenha caractere a caractere com spacing
        total_w = 0
        for c in text:
            cw, _ = text_size(draw, c, fnt)
            total_w += cw + letter_spacing
        total_w -= letter_spacing
        x = (canvas_w - total_w) // 2
        for c in text:
            draw.text((x, y), c, font=fnt, fill=color)
            cw, _ = text_size(draw, c, fnt)
            x += cw + letter_spacing

def diagonal_stripes(canvas, color, opacity=30, gap=80, angle_offset=0):
    """Desenha listras diagonais sutis (referência logo do festival)."""
    w, h = canvas.size
    overlay = Image.new("RGBA", canvas.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay)
    for x in range(-h, w + h, gap):
        d.line([(x, 0), (x + h, h)], fill=color + (opacity,), width=2)
    canvas.alpha_composite(overlay)

# ------------- LOGO COMPACTA -------------

def make_logo_compact(target_h):
    """Carrega logo do festival e redimensiona pra altura target."""
    logo = Image.open(os.path.join(ROOT, "logo-festival.png.png")).convert("RGBA")
    # Remove fundo preto se houver (a logo é fundo escuro + texto branco/verde)
    # Vamos manter o logo como está (já tem fundo preto, mas vamos usar como objeto)
    # Em vez disso, vamos remover o fundo preto pra ficar com fundo transparente.
    data = logo.getdata()
    new = []
    for r, g, b, a in data:
        # Pixels pretos viram transparente
        if r < 30 and g < 30 and b < 30:
            new.append((r, g, b, 0))
        else:
            new.append((r, g, b, a))
    logo.putdata(new)
    # Resize
    ratio = target_h / logo.size[1]
    new_w = int(logo.size[0] * ratio)
    return logo.resize((new_w, target_h), Image.LANCZOS)

# ------------- CARDS DE ARTISTA -------------

def build_artist_card(src_img_or_pil, name, label, card_w, card_h, photo_offset=0.0,
                      focus_y=0.5):
    """
    Constrói card de artista no estilo polaroid:
    - Foto ocupa 75% do card (em cima)
    - Bloco verde neon ocupa 25% (em baixo) com nome + data
    - focus_y: 0.0=topo, 0.5=centro, 1.0=baixo. Define onde focar a foto na vertical.
    """
    photo_h = int(card_h * 0.75)
    name_h_area = card_h - photo_h

    if isinstance(src_img_or_pil, str):
        src_img = Image.open(src_img_or_pil).convert("RGB")
    else:
        src_img = src_img_or_pil.convert("RGB")

    # Crop com foco vertical configurável
    w, h = src_img.size
    target_ratio = card_w / photo_h
    src_ratio = w / h
    if src_ratio > target_ratio:
        # mais larga: corta laterais
        new_w = int(h * target_ratio)
        left = (w - new_w) // 2
        src_img = src_img.crop((left, 0, left + new_w, h))
    else:
        # mais alta: aplica focus_y
        new_h = int(w / target_ratio)
        ideal_top = int(h * focus_y) - new_h // 2
        top = max(0, min(ideal_top, h - new_h))
        src_img = src_img.crop((0, top, w, top + new_h))
    photo = src_img.resize((card_w, photo_h), Image.LANCZOS)

    # Monta card final
    card = Image.new("RGBA", (card_w, card_h), (0, 0, 0, 255))
    card.paste(photo, (0, 0))
    # Bloco verde com nome
    d = ImageDraw.Draw(card)
    d.rectangle([(0, photo_h), (card_w, card_h)], fill=NEON)
    # Mask arredondada
    radius = int(card_w * 0.07)
    mask = rounded_mask((card_w, card_h), radius=radius)
    card.putalpha(mask)

    # Texto no bloco verde — autosize pra caber sempre (altura E largura)
    d = ImageDraw.Draw(card)
    # Cap inicial: o menor entre 18% da largura e 50% da altura do bloco
    name_size = min(int(card_w * 0.18), int(name_h_area * 0.55))
    while name_size > 18:
        fnt_name = font(F_DISPLAY, name_size)
        name_w, name_th = text_size(d, name, fnt_name)
        if name_w <= card_w - int(card_w * 0.10) * 2 and name_th <= int(name_h_area * 0.60):
            break
        name_size -= 2
    fnt_label = font(F_BEBAS, max(int(name_h_area * 0.25), 14))
    label_w, label_th = text_size(d, label, fnt_label)

    # Centro vertical do bloco
    block_center_y = photo_h + name_h_area // 2
    total_text_h = name_th + 4 + label_th
    text_top = block_center_y - total_text_h // 2 - 4
    # Centralizado horizontal
    name_x = (card_w - name_w) // 2
    label_x = (card_w - label_w) // 2
    d.text((name_x, text_top), name, font=fnt_name, fill=BLACK)
    d.text((label_x, text_top + name_th + 4), label, font=fnt_label, fill=BLACK)

    return card

# ------------- COMPOSIÇÃO BASE DO FUNDO -------------

def make_background(w, h):
    """Fundo preto com listras diagonais sutis verde neon."""
    bg = Image.new("RGBA", (w, h), BLACK + (255,))
    # Gradient radial sutil verde no centro
    overlay = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    cx, cy = w // 2, int(h * 0.45)
    max_r = max(w, h)
    # gradiente radial verde escuro
    for r in range(max_r, 0, -40):
        alpha = int(40 * (1 - r / max_r) ** 2)
        od.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(60, 110, 20, alpha))
    bg.alpha_composite(overlay)
    # Listras diagonais
    diagonal_stripes(bg, NEON, opacity=18, gap=100)
    return bg

# ------------- ARTE 1080x1080 -------------

def make_feed_1080():
    W, H = 1080, 1080
    canvas = make_background(W, H)
    d = ImageDraw.Draw(canvas)

    # Logo no topo
    logo = make_logo_compact(110)
    logo_w = logo.size[0]
    canvas.alpha_composite(logo, ((W - logo_w) // 2, 35))

    # Headline
    fnt_h1 = font(F_DISPLAY, 78)
    headline_y = 175
    draw_text_centered(d, "OS MELHORES SHOWS", fnt_h1, headline_y, WHITE, W)
    draw_text_centered(d, "DO ANO ESTÃO AQUI", fnt_h1, headline_y + 80, NEON, W)

    # Linha divisória
    d.line([(W // 2 - 200, headline_y + 180), (W // 2 + 200, headline_y + 180)],
           fill=NEON, width=3)

    # 3 cards de artistas (em linha) — ajustado pra caber tudo em 1080
    card_w, card_h = 290, 380
    gap = 25
    total_w = 3 * card_w + 2 * gap
    start_x = (W - total_w) // 2
    cards_y = 380

    matue_img = crop_matue_from_banner()
    matue_card = build_artist_card(matue_img, "MATUÊ", "28 / AGO",
                                   card_w, card_h, focus_y=0.30)
    jarule_card = build_artist_card(os.path.join(RAW, "aurora-galeria-2.jpg"),
                                    "JA RULE", "29 / AGO",
                                    card_w, card_h, focus_y=0.32)
    thiag_card = build_artist_card(os.path.join(RAW, "thiaguinho-lastfm-2.jpg"),
                                   "THIAGUINHO", "16 / AGO",
                                   card_w, card_h, focus_y=0.40)

    canvas.alpha_composite(matue_card, (start_x, cards_y))
    canvas.alpha_composite(jarule_card, (start_x + card_w + gap, cards_y))
    canvas.alpha_composite(thiag_card, (start_x + 2 * (card_w + gap), cards_y))

    # Preço + CTA
    price_y = cards_y + card_h + 30
    fnt_small = font(F_BEBAS, 34)
    draw_text_centered(d, "A PARTIR DE", fnt_small, price_y, GRAY, W)
    # R$ 108,00
    rs_fnt = font(F_BEBAS, 52)
    val_fnt = font(F_DISPLAY, 120)
    cents_fnt = font(F_BEBAS, 52)
    rs_w, rs_h = text_size(d, "R$", rs_fnt)
    val_w, val_h = text_size(d, "108", val_fnt)
    cents_w, cents_h = text_size(d, ",00", cents_fnt)
    total_price_w = rs_w + 16 + val_w + 4 + cents_w
    px = (W - total_price_w) // 2
    py = price_y + 38
    d.text((px, py + (val_h - rs_h)), "R$", font=rs_fnt, fill=NEON)
    d.text((px + rs_w + 16, py - 25), "108", font=val_fnt, fill=NEON)
    d.text((px + rs_w + 16 + val_w + 4, py + (val_h - cents_h) - 25),
           ",00", font=cents_fnt, fill=NEON)

    # CTA "COMPRE AGORA" - botão
    cta_text = "COMPRE AGORA  →"
    fnt_cta = font(F_DISPLAY, 48)
    cta_w, cta_h = text_size(d, cta_text, fnt_cta)
    btn_pad_x, btn_pad_y = 56, 22
    btn_w = cta_w + 2 * btn_pad_x
    btn_h = cta_h + 2 * btn_pad_y
    btn_x = (W - btn_w) // 2
    btn_y = py + val_h + 5
    d.rounded_rectangle([(btn_x, btn_y), (btn_x + btn_w, btn_y + btn_h)],
                        radius=btn_h // 2, fill=NEON)
    d.text((btn_x + btn_pad_x, btn_y + btn_pad_y - 6),
           cta_text, font=fnt_cta, fill=BLACK)

    # Save
    out_path = os.path.join(OUT, "suhai-festival-interlagos-1080x1080.png")
    canvas.convert("RGB").save(out_path, "PNG", optimize=True)
    print(f"Salvo: {out_path}")
    return out_path

# ------------- ARTE 1080x1920 (STORY/REELS) -------------

def make_story_1080():
    W, H = 1080, 1920
    canvas = make_background(W, H)
    d = ImageDraw.Draw(canvas)

    # Logo no topo
    logo = make_logo_compact(125)
    logo_w = logo.size[0]
    canvas.alpha_composite(logo, ((W - logo_w) // 2, 60))

    # Headline
    fnt_h1 = font(F_DISPLAY, 88)
    headline_y = 230
    draw_text_centered(d, "OS MELHORES", fnt_h1, headline_y, WHITE, W)
    draw_text_centered(d, "SHOWS DO ANO", fnt_h1, headline_y + 90, NEON, W)
    # Linha divisória
    d.line([(W // 2 - 180, headline_y + 200), (W // 2 + 180, headline_y + 200)],
           fill=NEON, width=3)

    matue_img = crop_matue_from_banner()

    # HERO CARD - JA RULE em destaque
    hero_w, hero_h = 880, 600
    hero_x = (W - hero_w) // 2
    hero_y = 500
    # Tag sobre o card: "ATRAÇÃO INTERNACIONAL"
    fnt_intl = font(F_BEBAS, 32)
    draw_text_centered(d, "★  ATRAÇÃO INTERNACIONAL  ★",
                       fnt_intl, hero_y - 42, NEON, W)
    jarule_card = build_artist_card(os.path.join(RAW, "aurora-galeria-2.jpg"),
                                    "JA RULE", "29 / AGO  ·  EUA",
                                    hero_w, hero_h, focus_y=0.30)
    canvas.alpha_composite(jarule_card, (hero_x, hero_y))

    # 2 cards menores side-by-side: MATUÊ + THIAGUINHO
    small_w, small_h = 425, 420
    small_gap = 30
    smalls_y = hero_y + hero_h + 35
    smalls_total = 2 * small_w + small_gap
    small_x = (W - smalls_total) // 2

    matue_card = build_artist_card(matue_img, "MATUÊ", "28 / AGO",
                                   small_w, small_h, focus_y=0.30)
    canvas.alpha_composite(matue_card, (small_x, smalls_y))

    thiag_card = build_artist_card(os.path.join(RAW, "thiaguinho-lastfm-2.jpg"),
                                   "THIAGUINHO", "16 / AGO",
                                   small_w, small_h, focus_y=0.40)
    canvas.alpha_composite(thiag_card, (small_x + small_w + small_gap, smalls_y))

    # Preço + CTA — compacto pra caber
    price_y = smalls_y + small_h + 30
    fnt_small = font(F_BEBAS, 34)
    draw_text_centered(d, "A PARTIR DE", fnt_small, price_y, GRAY, W)
    rs_fnt = font(F_BEBAS, 52)
    val_fnt = font(F_DISPLAY, 120)
    cents_fnt = font(F_BEBAS, 52)
    rs_w, rs_h = text_size(d, "R$", rs_fnt)
    val_w, val_h = text_size(d, "108", val_fnt)
    cents_w, cents_h = text_size(d, ",00", cents_fnt)
    total_price_w = rs_w + 16 + val_w + 6 + cents_w
    px = (W - total_price_w) // 2
    py = price_y + 38
    d.text((px, py + (val_h - rs_h) - 4), "R$", font=rs_fnt, fill=NEON)
    d.text((px + rs_w + 16, py - 28), "108", font=val_fnt, fill=NEON)
    d.text((px + rs_w + 16 + val_w + 6, py + (val_h - cents_h) - 28),
           ",00", font=cents_fnt, fill=NEON)

    # CTA
    cta_text = "COMPRE AGORA  →"
    fnt_cta = font(F_DISPLAY, 54)
    cta_w, cta_h = text_size(d, cta_text, fnt_cta)
    btn_pad_x, btn_pad_y = 64, 24
    btn_w = cta_w + 2 * btn_pad_x
    btn_h = cta_h + 2 * btn_pad_y
    btn_x = (W - btn_w) // 2
    btn_y = py + val_h - 10
    d.rounded_rectangle([(btn_x, btn_y), (btn_x + btn_w, btn_y + btn_h)],
                        radius=btn_h // 2, fill=NEON)
    d.text((btn_x + btn_pad_x, btn_y + btn_pad_y - 6),
           cta_text, font=fnt_cta, fill=BLACK)

    # Save
    out_path = os.path.join(OUT, "suhai-festival-interlagos-1080x1920.png")
    canvas.convert("RGB").save(out_path, "PNG", optimize=True)
    print(f"Salvo: {out_path}")
    return out_path


if __name__ == "__main__":
    make_feed_1080()
    make_story_1080()
    print("Concluído.")
