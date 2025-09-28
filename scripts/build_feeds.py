# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Cambio clave: cuotas por fuente EXACTAS + filtro de prefijo de path por fuente.

import os, re, html, hashlib, traceback, random
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.1; +https://github.com/)"}

# === FUENTES con CUOTA (links exactos) ===
SOURCES = {
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
        ("https://www.diariolibre.com/rss/portada.xml", 3),
        ("https://listindiario.com/la-republica", 3),
        ("https://www.elcaribe.com.do/seccion/panorama/pais/", 3),
        ("https://eldinero.com.do/", 3),
    ],
}

# Límite total por país (suma de cuotas)
LIMITS = {"venezuela": 15, "panama": 10, "dominicana": 12}

# Cuota máxima por dominio dentro de cada país (ya no se usa para mezclar cuotas fijas, se deja por compatibilidad)
MAX_PER_DOMAIN = 4

# Selectores por dominio (para scraping HTML)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "article h3 a", "h1 a[href]", "h2 a[href]", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a", ".jeg_post_title a", ".post-title a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']", ".jeg_post_title a", ".post-title a"],

    # Panamá
    "www.prensa.com": [
        "h1 a[href^='https://www.prensa.com/']",
        "h2 a[href^='https://www.prensa.com/']",
        "article h2 a[href^='https://www.prensa.com/']",
        "a[href^='https://www.prensa.com/']:not([href*='/tag/']):not([href*='autor'])",
    ],
    "www.laestrella.com.pa": [
        "h1 a[href^='https://www.laestrella.com.pa/']",
        "h2 a[href^='https://www.laestrella.com.pa/']",
        "article h2 a[href^='https://www.laestrella.com.pa/']",
        "a[href^='https://www.laestrella.com.pa/']:not([href*='/etiquetas/'])",
    ],

    # Dominicana
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a", ".post-title a"],
    # refuerzo para El Caribe:
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]", ".entry-title a[href]", ".td-module-title a[href]", ".c-post-card a[href]"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]", ".entry-title a[href]"],
}

# ---------- Utilidades ----------
def log(msg: str): print(msg, flush=True)

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query)
              if not k.lower().startswith("utm") and k.lower() not in {"gclid","fbclid"}]
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path, "", urlencode(qs), ""))
    except Exception:
        return u

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def abs_url(base: str, href: str) -> str:
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def get_html(url: str) -> bytes | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            # asegurar decodificación correcta
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r.text.encode(r.encoding or "utf-8", errors="ignore")
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

def same_host_and_path_prefix(link: str, base_url: str) -> bool:
    """
    Acepta SOLO si:
    - El host es el mismo.
    - El path del link empieza con el path de la fuente (prefijo exacto) o es igual (sin barra final).
    """
    try:
        pl = urlparse(clean_url(link))
        pb = urlparse(clean_url(base_url))
        if pl.netloc.lower() != pb.netloc.lower():
            return False
        base_path = pb.path if pb.path.endswith("/") else pb.path + "/"
        return pl.path.startswith(base_path) or pl.path == pb.path
    except Exception:
        return False

# ---------- Scraping/RSS ----------
def scrape_site(url: str, quota: int) -> list[dict]:
    """
    Scrapea SOLO la URL dada y devuelve HASTA `quota` items que:
      - Sean del mismo host
      - Y cuyo path EMPIECE por el path de la fuente (link exacto + subrutas)
    """
    out: list[dict] = []
    try:
        soup = fetch_html(url)
        if not soup:
            log(f"[WARN] No HTML for {url}")
            return out
        parsed = urlparse(url)
        host, base = parsed.netloc.lower(), f"{parsed.scheme}://{parsed.netloc}/"
        selectors = SITE_SELECTORS.get(host, []) + ["h1 a[href]", "h2 a[href]", "article h2 a"]

        seen = set()
        for sel in selectors:
            hits = 0
            for tag in soup.select(sel):
                link = tag.get("href")
                text = tag.get_text(strip=True)
                if not link:
                    a = tag.find("a", href=True)
                    if a:
                        link = a["href"]
                        if not text:
                            text = a.get_text(strip=True)
                if not text or not link:
                    continue
                full = abs_url(base, link)
                # FILTRO CLAVE: host + prefijo de path EXACTO
                if not same_host_and_path_prefix(full, url):
                    continue
                if full in seen: 
                    continue
                seen.add(full)

                # filtros básicos anti-basura
                path = urlparse(full).path.lower()
                if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category", "/nosotros", "/about")):
                    continue
                if path.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
                    continue
                if len(text) < 8:
                    continue

                out.append({
                    "title": text,
                    "link": clean_url(full),
                    "date": datetime.utcnow(),
                    "source": host,
                    "domain": host
                })
                hits += 1
                if len(out) >= quota: 
                    break
            log(f"[INFO] {host} selector '{sel}' → {hits}")
            if len(out) >= quota: 
                break

        # Fallback mínimo si no llenó cuota (anclas genéricas PERO manteniendo el mismo filtro exacto)
        if len(out) < quota:
            fb = 0
            for a in soup.select("a[href]"):
                if len(out) >= quota: break
                href = a.get("href",""); txt = a.get_text(strip=True)
                if not href or not txt: continue
                full = abs_url(base, href)
                if not same_host_and_path_prefix(full, url): continue
                if full in {it["link"] for it in out}: continue
                if len(txt) < 12: continue
                path = urlparse(full).path.lower()
                if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category", "/nosotros", "/about")):
                    continue
                if path.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg")):
                    continue
                out.append({"title": txt, "link": clean_url(full), "date": datetime.utcnow(),
                            "source": host, "domain": host})
                fb += 1
            log(f"[INFO] {host} fallback anchors → {fb}")

        # Dedupe dentro de la fuente (por si el mismo enlace sale dos veces)
        seenk, dedup = set(), []
        for it in out:
            key = hashlib.md5((norm_text(it["title"])+"|"+urlparse(it["link"]).netloc).encode()).hexdigest()
            if key in seenk: continue
            seenk.add(key)
            dedup.append(it)

        return dedup[:quota]
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

def fetch_rss(url: str, cutoff_utc: datetime, quota: int) -> list[dict]:
    out: list[dict] = []
    try:
        c = get_html(url)
        if not c:
            log(f"[WARN] No RSS for {url}")
            return out
        feed = feedparser.parse(c)
        for e in feed.entries:
            if len(out) >= quota: break
            title = getattr(e,"title","") or ""
            link  = getattr(e,"link","") or ""
            if not title or not link: continue
            if getattr(e,"published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e,"updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff_utc: continue
            dom = urlparse(link).netloc or urlparse(url).netloc
            out.append({"title": title, "link": clean_url(link), "date": dt,
                        "source": dom, "domain": dom})
        log(f"[INFO] RSS {url} → {len(out)} after cutoff")
        return out[:quota]
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

# ---------- Ensamblado (cuotas fijas por fuente; sin mixing global) ----------
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

# ---------- Pipeline por país ----------
def generate_country_feed(country: str, sources_with_quotas: list[tuple[str,int]], limit_total: int):
    log(f"[RUN] Generating {country} (target total {limit_total})")
    items, cutoff = [], datetime.utcnow() - timedelta(days=1)

    # Recolecta EXACTAMENTE por fuente y cuota
    for url, quota in sources_with_quotas:
        try:
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff, quota)
            else:
                got = scrape_site(url, quota)
            # etiqueta dominio (por si falta)
            for it in got:
                it["domain"] = it.get("domain") or urlparse(it["link"]).netloc.lower()
            items += got
            log(f"[INFO] {country} source {url} → {len(got)}/{quota}")
        except Exception:
            log(f"[ERROR] {country} source {url} crashed:\n{traceback.format_exc()}")

    # Dedup global por link (si alguna fuente repite)
    seen = set(); final = []
    for it in items:
        if it["link"] in seen: 
            continue
        seen.add(it["link"])
        final.append(it)

    write_feed(country, final)

def main():
    random.seed()
    for country, urls in SOURCES.items():
        try:
            generate_country_feed(country, urls, LIMITS[country])
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")
            write_feed(country, [{
                "title": f"Error generando {country}",
                "link": "https://example.com/",
                "date": datetime.utcnow(),
                "source": "generator",
                "domain": "generator"
            }])

if __name__ == "__main__":
    main()
