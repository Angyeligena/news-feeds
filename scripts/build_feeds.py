# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Exact-match por URL (mismo host + prefijo de path) y CUOTAS por fuente.

import os, re, html, hashlib, traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.2; +https://github.com/)",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

# ========================== FUENTES Y CUOTAS ==========================
# Usa EXACTAMENTE estas URLs y saca este número de noticias por fuente.
SOURCES_BY_COUNTRY = {
    "venezuela": [
        ("https://www.elnacional.com/venezuela/", 5),
        ("https://talcualdigital.com/noticias/", 5),
        ("https://efectococuyo.com/politica/", 5),
    ],
    "panama": [
        ("https://www.prensa.com/", 5),
        ("https://www.laestrella.com.pa/panama", 5),
    ],
    "dominicana": [
        ("https://www.diariolibre.com/rss/portada.xml", 3),  # RSS
        ("https://listindiario.com/la-republica", 3),
        ("https://www.elcaribe.com.do/seccion/panorama/pais/", 3),
        ("https://eldinero.com.do/", 3),
    ],
}

# ========================== SELECTORES OPCIONALES ==========================
SITE_SELECTORS = {
    # VE
    "www.elnacional.com": ["article h2 a", "h1 a[href]", "h2 a[href]"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "h2 a[href]"],
    "efectococuyo.com": ["article h2 a", "h2 a[href]", ".jeg_post_title a"],
    # PA
    "www.prensa.com": [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]",
    ],
    "www.laestrella.com.pa": [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]",
    ],
    # DO
    "listindiario.com": ["article h2 a", "h2 a[href]", "h3 a[href]"],
    "www.elcaribe.com.do": ["article h2 a", "h2 a[href]", "a.post-title[href]"],
    "eldinero.com.do": ["article h2 a", "h2 a[href]", "a.post-title[href]"],
}

# ============================= UTILIDADES =============================
def log(msg: str): print(msg, flush=True)

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query)
              if not k.lower().startswith("utm") and k.lower() not in {"gclid","fbclid"}]
        # conservar path tal cual; no recortamos "/" final para no romper prefijos
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u

def abs_url(base: str, href: str) -> str:
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def same_host_and_path_prefix(link: str, base_url: str) -> bool:
    """
    Acepta SOLO si:
    - El host es el mismo.
    - El path del link empieza con el path de la fuente (prefijo exacto).
    """
    try:
        pl = urlparse(clean_url(link))
        pb = urlparse(clean_url(base_url))
        if pl.netloc.lower() != pb.netloc.lower():
            return False
        # normalizamos: asegurar que el prefijo termina con "/"
        base_path = pb.path if pb.path.endswith("/") else pb.path + "/"
        link_path = pl.path
        return link_path.startswith(base_path) or link_path == pb.path
    except Exception:
        return False

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

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

# ============================= EXTRACCIÓN =============================
def scrape_exact(url: str, quota: int) -> list[dict]:
    """
    Scrapea SOLO la página dada y devuelve hasta `quota` items
    cuyos enlaces empiecen por el MISMO host y el MISMO path (prefijo).
    """
    out: list[dict] = []
    resp = get_html(url)
    if not resp:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    base = f"{parsed.scheme}://{parsed.netloc}"

    selectors = SITE_SELECTORS.get(host, []) + [
        "article h2 a[href]", "h1 a[href]", "h2 a[href]"
    ]
    seen_links = set()

    for sel in selectors:
        for tag in soup.select(sel):
            href = tag.get("href")
            title = tag.get_text(strip=True)
            if not href or not title:
                continue
            full = clean_url(abs_url(base, href))
            # FILTRO CLAVE: mismo host + path prefijo EXACTO de la fuente
            if not same_host_and_path_prefix(full, url):
                continue
            if full in seen_links:
                continue
            seen_links.add(full)
            # filtros muy suaves
            path = urlparse(full).path.lower()
            if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category")):
                continue
            if len(title) < 8:
                continue
            out.append({
                "title": title,
                "link": full,
                "date": datetime.utcnow(),
                "domain": host,
            })
            if len(out) >= quota:
                break
        if len(out) >= quota:
            break

    # Si no llegamos al cupo, intentamos un fallback muy conservador:
    if len(out) < quota:
        for a in soup.select("a[href]"):
            if len(out) >= quota: break
            href = a.get("href"); title = a.get_text(strip=True)
            if not href or not title: continue
            full = clean_url(abs_url(base, href))
            if not same_host_and_path_prefix(full, url): continue
            if full in {it["link"] for it in out}: continue
            if len(title) < 12: continue
            path = urlparse(full).path.lower()
            if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category")):
                continue
            out.append({
                "title": title,
                "link": full,
                "date": datetime.utcnow(),
                "domain": host,
            })

    return out[:quota]

def fetch_rss_exact(url: str, quota: int) -> list[dict]:
    """
    Lee un RSS y devuelve hasta `quota` items.
    (No aplicamos filtro de path para RSS porque ya apunta a la sección correcta.)
    """
    out: list[dict] = []
    try:
        r = get_html(url)
        if not r:
            return out
        feed = feedparser.parse(r.content)
        cutoff = datetime.utcnow() - timedelta(days=2)  # ligera ventana por si publican lento
        for e in feed.entries:
            title = getattr(e, "title", "") or ""
            link = getattr(e, "link", "") or ""
            if not title or not link:
                continue
            if getattr(e, "published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e, "updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff:
                continue
            out.append({
                "title": title,
                "link": clean_url(link),
                "date": dt,
                "domain": urlparse(link).netloc.lower() or urlparse(url).netloc.lower(),
            })
            if len(out) >= quota:
                break
    except Exception:
        log(f"[ERROR] fetch_rss_exact({url}) crashed:\n{traceback.format_exc()}")
    return out[:quota]

# ============================= ENSAMBLADO =============================
def dedupe_keep_order(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for it in items:
        if it["link"] in seen:
            continue
        seen.add(it["link"])
        out.append(it)
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

def generate_country_feed(country: str, sources_with_quotas: list[tuple[str,int]]):
    log(f"[RUN] {country}")
    collected: list[dict] = []

    for src_url, quota in sources_with_quotas:
        try:
            if src_url.endswith(".xml") or "rss" in src_url.lower() or "feed" in src_url.lower():
                got = fetch_rss_exact(src_url, quota)
            else:
                got = scrape_exact(src_url, quota)
            log(f"[INFO] {country} | {src_url} → {len(got)}/{quota}")
            collected.extend(got)
        except Exception:
            log(f"[ERROR] {country} source {src_url} crashed:\n{traceback.format_exc()}")

    # Dedup global (por si alguna fuente repite el mismo enlace)
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
