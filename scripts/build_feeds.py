# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# EXACT URLs + cuotas por fuente. Arregla Venezuela y El Caribe (RD).

import os, re, html, traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AngieNewsBot/1.4",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# =================== FUENTES / CUOTAS / PREFIJO ===================
# strict_prefix: True => exige que el path del enlace empiece por el path base.
# allow_regex: patrones "whitelist" adicionales para aceptar enlaces del mismo host (cuando strict=False).
SOURCES_BY_COUNTRY = {
    "venezuela": [
        # El Nacional: prefijo /venezuela/ ESTRICTO
        ("https://www.elnacional.com/venezuela/", 5, True,  None),
        # TalCual: enlaces van a /politica/, /nacional/, /202x/ → host igual, sin prefijo estricto, con whitelist
        ("https://talcualdigital.com/noticias/",  5, False, r"(^/20\d{2}/|/politica/|/nacional/|/venezuela/)"),
        # Efecto Cocuyo: similar → host igual, whitelist
        ("https://efectococuyo.com/politica/",    5, False, r"(^/20\d{2}/|/politica/|/venezuela/|/economia/)"),
    ],
    "panama": [
        ("https://www.prensa.com/",               5, True,  None),
        ("https://www.laestrella.com.pa/panama",  5, True,  None),
    ],
    "dominicana": [
        ("https://www.diariolibre.com/rss/portada.xml", 3, True,  None),  # RSS
        ("https://listindiario.com/la-republica",       3, True,  None),
        # El Caribe: prefijo estricto /seccion/panorama/pais/
        ("https://www.elcaribe.com.do/seccion/panorama/pais/", 3, True,  None),
        ("https://eldinero.com.do/",                     3, True,  None),
    ],
}

# =================== SELECTORES REFORZADOS ===================
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": [
        "article h2 a[href]", "article h3 a[href]", "h1 a[href]", "h2 a[href]",
        ".headline a[href]", ".post-title a[href]", ".card__link[href]", ".entry-title a[href]"
    ],
    "talcualdigital.com": [
        "h2.entry-title a[href]", "article h2 a[href]", "div.post-title h2 a[href]",
        ".jeg_post_title a[href]", ".post-title a[href]", "h3 a[href]", ".entry-title a[href]"
    ],
    "efectococuyo.com": [
        "article h2 a[href]", "h2 a[href]", "h3 a[href]", ".jeg_post_title a[href]",
        ".post-title a[href]", ".entry-title a[href]"
    ],
    # Panamá
    "www.prensa.com": [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]", ".headline a[href]", ".entry-title a[href]"
    ],
    "www.laestrella.com.pa": [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]", ".headline a[href]", ".entry-title a[href]"
    ],
    # Dominicana
    "listindiario.com": [
        "article h2 a[href]", "h2 a[href]", "h3 a[href]", ".post-title a[href]", ".headline a[href]", ".entry-title a[href]"
    ],
    "www.elcaribe.com.do": [
        # Varias plantillas de El Caribe
        "article h2 a[href]", "h2.entry-title a[href]", "h3.entry-title a[href]",
        ".post-title a[href]", ".td-module-title a[href]", ".c-post-card a[href]",
        ".entry-title a[href]", ".headline a[href]"
    ],
    "eldinero.com.do": [
        "article h2 a[href]", "h2 a[href]", "h3 a[href]", "a.post-title[href]", ".post-title a[href]", ".entry-title a[href]"
    ],
}

# =================== FILTROS ===================
# Paths irrelevantes típicos que debemos excluir
BAD_PATH_SEGMENTS = (
    "/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category",
    "/nosotros", "/quienes-somos", "/about", "/terminos", "/aviso", "/privacy",
    "/contacto", "/clasificados", "/suscripciones", "/newsletter", "/foro",
    "/edicion-impresa", "/impresa", "/archivo"
)
# extensiones no-noticia
BAD_EXTENSIONS = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")

# =================== UTIL ===================
def log(msg: str): print(msg, flush=True)

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query)
              if not k.lower().startswith("utm") and k.lower() not in {"gclid","fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u

def abs_url(base: str, href: str) -> str:
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def same_host(link: str, base_url: str) -> bool:
    pl = urlparse(clean_url(link)); pb = urlparse(clean_url(base_url))
    return pl.netloc.lower() == pb.netloc.lower()

def path_starts_with_prefix(link: str, base_url: str) -> bool:
    pl = urlparse(clean_url(link)); pb = urlparse(clean_url(base_url))
    base_path = pb.path if pb.path.endswith("/") else pb.path + "/"
    return pl.path.startswith(base_path) or pl.path == pb.path

def is_probably_article(link: str, title: str, host: str) -> bool:
    """Filtro simple anti-páginas de ‘nosotros’, PDFs, etc."""
    if not title or len(title.strip()) < 8: return False
    p = urlparse(link)
    if p.netloc.lower() != host: return False
    path = p.path.lower()
    if any(seg in path for seg in BAD_PATH_SEGMENTS): return False
    if path.endswith(BAD_EXTENSIONS): return False
    # evitar home
    if path in ("/", "", "/home"): return False
    return True

def get_html(url: str) -> requests.Response | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.0)
    return None

# =================== EXTRACCIÓN ===================
def scrape_source(url: str, quota: int, strict_prefix: bool, allow_regex: str | None) -> list[dict]:
    """
    Devuelve hasta `quota` items desde `url`.
    - strict_prefix=True: requiere mismo host + path con el prefijo exacto.
    - strict_prefix=False: requiere mismo host; adicionalmente, si allow_regex está presente,
      el path debe coincidir con ese patrón (lista blanca).
    """
    out: list[dict] = []
    resp = get_html(url)
    if not resp:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")

    pb = urlparse(url)
    host = pb.netloc.lower()
    base = f"{pb.scheme}://{pb.netloc}"
    allow_pat = re.compile(allow_regex) if allow_regex else None

    selectors = SITE_SELECTORS.get(host, []) + [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]"
    ]

    seen_links = set()

    def accept_link(full_link: str, title: str) -> bool:
        if not same_host(full_link, url): 
            return False
        if strict_prefix and not path_starts_with_prefix(full_link, url):
            return False
        if not strict_prefix and allow_pat:
            # cuando no somos estrictos, exigimos que el path cumpla whitelist
            path = urlparse(full_link).path
            if not allow_pat.search(path):
                return False
        return is_probably_article(full_link, title, host)

    for sel in selectors:
        hits = 0
        for tag in soup.select(sel):
            href = tag.get("href")
            title = tag.get_text(strip=True)
            if not href or not title:
                continue
            full = clean_url(abs_url(base, href))
            if full in seen_links:
                continue
            if not accept_link(full, title):
                continue
            seen_links.add(full)
            out.append({
                "title": title.strip(),
                "link": full,
                "date": datetime.utcnow(),
                "domain": host,
            })
            hits += 1
            if len(out) >= quota:
                break
        log(f"[INFO] {host} selector '{sel}' → {hits}")
        if len(out) >= quota:
            break

    # Fallback si no llenó la cuota (anchors genéricos pero con los mismos filtros)
    if len(out) < quota:
        for a in soup.select("a[href]"):
            if len(out) >= quota: break
            href = a.get("href"); title = a.get_text(strip=True)
            if not href or not title: continue
            full = clean_url(abs_url(base, href))
            if full in seen_links: continue
            if not accept_link(full, title): continue
            seen_links.add(full)
            out.append({
                "title": title.strip(),
                "link": full,
                "date": datetime.utcnow(),
                "domain": host,
            })

    return out[:quota]

def fetch_rss_exact(url: str, quota: int) -> list[dict]:
    out: list[dict] = []
    try:
        r = get_html(url)
        if not r: return out
        feed = feedparser.parse(r.content)
        cutoff = datetime.utcnow() - timedelta(days=2)
        for e in feed.entries:
            title = getattr(e, "title", "") or ""
            link  = getattr(e, "link", "") or ""
            if not title or not link: continue
            if getattr(e, "published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e, "updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff: continue
            out.append({
                "title": title.strip(),
                "link": clean_url(link),
                "date": dt,
                "domain": urlparse(link).netloc.lower() or urlparse(url).netloc.lower(),
            })
            if len(out) >= quota:
                break
    except Exception:
        log(f"[ERROR] fetch_rss_exact({url}) crashed:\n{traceback.format_exc()}")
    return out[:quota]

# =================== ENSAMBLADO/ESCRITURA ===================
def dedupe_keep_order(items: list[dict]) -> list[dict]:
    seen = set(); out = []
    for it in items:
        if it["link"] in seen: continue
        seen.add(it["link"]); out.append(it)
    return out

def make_rss(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    build_comment = f"<!-- build {datetime.utcnow().isoformat()}Z -->"
    esc = lambda s: html.escape(s or "", quote=True)
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>', '<rss version="2.0">', '<channel>',
        f'  <title>Noticias {esc(country.title())}</title>',
        '  <link>https://github.com/</link>',
        '  <description>Feed generado automáticamente</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>',
        '  <generator>news-feeds GitHub Action</generator>',
        f'  {build_comment}',
    ]
    for it in items:
        parts += [
            '  <item>',
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            f"    <category>{esc(it['domain'])}</category>",
            '  </item>',
        ]
    parts += ['</channel>','</rss>','']
    return "\n".join(parts)

def write_feed(country: str, items: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{country}.xml")
    xml = make_rss(country, items)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(xml)
    log(f"[OK] Wrote {path} ({len(items)} items)")

def generate_country_feed(country: str, sources):
    log(f"[RUN] {country}")
    collected: list[dict] = []

    for src_url, quota, strict_prefix, allow_rx in sources:
        try:
            if src_url.endswith(".xml") or "rss" in src_url.lower() or "feed" in src_url.lower():
                got = fetch_rss_exact(src_url, quota)
            else:
                got = scrape_source(src_url, quota, strict_prefix, allow_rx)
            log(f"[INFO] {country} | {src_url} (strict={strict_prefix}) → {len(got)}/{quota}")
            collected.extend(got)
        except Exception:
            log(f"[ERROR] {country} source {src_url} crashed:\n{traceback.format_exc()}")

    final_items = dedupe_keep_order(collected)
    write_feed(country, final_items)

def main():
    for country, sources in SOURCES_BY_COUNTRY.items():
        try:
            generate_country_feed(country, sources)
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")
            write_feed(country, [])

if __name__ == "__main__":
    main()
