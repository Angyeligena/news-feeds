# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# Cambio clave: URLs exactas (host + prefijo de path) y cuotas por fuente.

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

# === FUENTES (url, cuota) EXACTAS ===
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

# Límite total por país = suma de cuotas por país
LIMITS = {"venezuela": 15, "panama": 10, "dominicana": 12}

# Selectores por dominio (para scraping HTML)
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "article h3 a", "h1 a[href]", "h2 a[href]", ".headline a", ".post-title a", ".entry-title a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a", ".jeg_post_title a", ".post-title a", "h3 a[href]", ".entry-title a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']", "h3 a[href]", ".jeg_post_title a", ".post-title a", ".entry-title a"],
    # Panamá
    "www.prensa.com": [
        "h1 a[href^='https://www.prensa.com/']",
        "h2 a[href^='https://www.prensa.com/']",
        "article h2 a[href^='https://www.prensa.com/']",
        "a[href^='https://www.prensa.com/']:not([href*='/tag/']):not([href*='autor'])",
        ".entry-title a[href^='https://www.prensa.com/']",
    ],
    "www.laestrella.com.pa": [
        "h1 a[href^='https://www.laestrella.com.pa/']",
        "h2 a[href^='https://www.laestrella.com.pa/']",
        "article h2 a[href^='https://www.laestrella.com.pa/']",
        "a[href^='https://www.laestrella.com.pa/']:not([href*='/etiquetas/'])",
        ".entry-title a[href^='https://www.laestrella.com.pa/']",
    ],
    # Dominicana
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a", ".post-title a[href]", ".headline a[href]", ".entry-title a[href]"],
    "www.elcaribe.com.do": [
        "article h2 a[href]", "h2.entry-title a[href]", "h3.entry-title a[href]",
        ".post-title a[href]", ".td-module-title a[href]", ".c-post-card a[href]",
        ".entry-title a[href]", ".headline a[href]"
    ],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]", ".post-title a[href]", ".entry-title a[href]"],
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

def same_host_and_prefix(link: str, base_url: str) -> bool:
    """Acepta solo si el host coincide y el path del enlace empieza por el path base (prefijo)."""
    try:
        pl = urlparse(clean_url(link))
        pb = urlparse(clean_url(base_url))
        if pl.netloc.lower() != pb.netloc.lower():
            return False
        base_path = pb.path if pb.path.endswith("/") else pb.path + "/"
        return pl.path.startswith(base_path) or pl.path == pb.path
    except Exception:
        return False

def get_html(url: str) -> bytes | None:
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            # reparar encoding básico
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r.content
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

# ---------- Scraping/RSS ----------
def scrape_site(url: str, quota: int) -> list[dict]:
    """
    Scrapea SOLO la página dada y devuelve hasta `quota` items
    que cumplan host igual + prefijo de path exacto.
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

                # FILTRO CLAVE: mismo host + prefijo exacto del path de la fuente
                if not same_host_and_prefix(full, url):
                    continue

                if full in seen: continue
                seen.add(full)
                # Filtrar ruido típico
                path = urlparse(full).path.lower()
                if any(seg in path for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category")):
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
                if len(out) >= quota: break
            log(f"[INFO] {host} selector '{sel}' → {hits}")
            if len(out) >= quota: break

        # Fallback si aún no llenó la cuota (anchors generales con el mismo filtro)
        if len(out) < quota:
            fb = 0
            for a in soup.select("a[href]"):
                if len(out) >= quota: break
                href = a.get("href",""); txt=a.get_text(strip=True); full=abs_url(base, href)
                if not same_host_and_prefix(full, url): continue
                if urlparse(full).netloc.lower()!=host or len(txt)<12: continue
                pth = urlparse(full).path.lower()
                if any(seg in pth for seg in ("/tag/", "/etiqueta/", "/autor", "/author", "/categoria", "/category")):
                    continue
                if full in {it["link"] for it in out}: continue
                out.append({"title": txt, "link": clean_url(full), "date": datetime.utcnow(),
                            "source": host, "domain": host})
                fb += 1
            log(f"[INFO] {host} fallback anchors → {fb}")

        # Dedupe dentro de la fuente (orden estable)
        seenk, dedup = set(), []
        for it in out:
            key = hashlib.md5((norm_text(it["title"])+"|"+urlparse(it["link"]).netloc).encode()).hexdigest()
            if key in seenk: continue
            seenk.add(key)
            dedup.append(it)
        return dedup[: quota]
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
            if len(out) >= quota:
                break
        log(f"[INFO] RSS {url} → {len(out)} after cutoff")
        return out
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

# ---------- Ensamblado (cuotas por fuente, sin mix por dominio) ----------
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

# ---------- Pipeline por país (cuotas por fuente) ----------
def generate_country_feed(country: str, sources_with_quota: list[tuple[str,int]], limit_total: int):
    log(f"[RUN] Generating {country} (limit {limit_total})")
    items, cutoff = [], datetime.utcnow() - timedelta(days=1)

    for url, quota in sources_with_quota:
        try:
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff, quota)
            else:
                got = scrape_site(url, quota)
            # etiqueta dominio (por si falta)
            for it in got:
                it["domain"] = it.get("domain") or urlparse(it["link"]).netloc.lower()
            log(f"[INFO] {country} source {url} → {len(got)}/{quota}")
            items.extend(got[:quota])
        except Exception:
            log(f"[ERROR] {country} source {url} crashed:\n{traceback.format_exc()}")

    # dedupe global (por seguridad)
    seen = set(); final = []
    for it in items:
        if it["link"] in seen: continue
        seen.add(it["link"]); final.append(it)

    # recorte defensivo por si excede el límite del país (ahora coincide con suma de cuotas)
    write_feed(country, final[:limit_total])

def main():
    random.seed()  # mantiene orden parcialmente variable entre runs (no crítico)
    for country, sources in SOURCES.items():
        try:
            generate_country_feed(country, sources, LIMITS[country])
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
