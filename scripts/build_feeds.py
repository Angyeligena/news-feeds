# scripts/build_feeds.py
# Genera RSS diarios para Venezuela, Panamá y Dominicana SIN Google News.
# Robusto: UA, retries, logs por fuente, placeholders, lastBuildDate, y NUNCA deja de escribir los XML.

import os, re, html, hashlib, traceback
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.0; +https://github.com/)"}

SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",
        "https://talcualdigital.com/",
        "https://efectococuyo.com/",
        "https://www.bloomberglinea.com/latinoamerica/venezuela/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/",
        "https://www.bloomberglinea.com/latinoamerica/panama/",
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada.xml",
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']"],
    "www.bloomberglinea.com": [
        "article a[href*='/venezuela/'] h2",
        "article a[href*='/panama/'] h2",
        "article h2 a",
        "article a[href] h2",
    ],
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
    "listindiario.com": ["h2 a[href]", "h3 a[href]", "article h2 a"],
    "www.elcaribe.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
    "eldinero.com.do": ["h2 a[href]", "article h2 a", "a.post-title[href]"],
}

def log(msg: str):
    print(msg, flush=True)

def clean_url(u: str) -> str:
    try:
        p = urlparse(u)
        qs = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith("utm") and k.lower() not in {"gclid","fbclid"}]
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
            return r.content
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.5)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    content = get_html(url)
    if not content: return None
    return BeautifulSoup(content, "html.parser")

def scrape_site(url: str, limit: int) -> list[dict]:
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
                if full in seen:
                    continue
                seen.add(full)
                out.append({"title": text, "link": full, "date": datetime.utcnow(), "source": host})
                hits += 1
                if len(out) >= limit * 4:
                    break
            log(f"[INFO] {host} selector '{sel}' → {hits} items")
            if len(out) >= limit * 4:
                break

        # Filtro de calidad
        before = len(out)
        out = [it for it in out if len(it["title"]) >= 25]
        log(f"[INFO] {host} filter titles >=25 chars: {before} → {len(out)}")

        # Fallback si vacío
        if not out:
            fallback_hits = 0
            for a in soup.select("a[href]"):
                href = a.get("href",""); txt = a.get_text(strip=True); full = abs_url(base, href)
                if urlparse(full).netloc.lower()!=host or len(txt)<35: continue
                out.append({"title": txt, "link": full, "date": datetime.utcnow(), "source": host})
                fallback_hits += 1
                if len(out) >= limit * 2: break
            log(f"[INFO] {host} fallback anchors → {fallback_hits} items")

        # Dedupe
        uniq, dedup = set(), []
        for it in out:
            key = hashlib.md5((norm_text(it["title"])+"|"+urlparse(it["link"]).netloc).encode()).hexdigest()
            if key in uniq: continue
            uniq.add(key)
            it["link"] = clean_url(it["link"])
            dedup.append(it)

        log(f"[INFO] {host} total after dedupe: {len(dedup)}")
        return dedup[: limit * 2]
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

def fetch_rss(url: str, cutoff_utc: datetime) -> list[dict]:
    out: list[dict] = []
    try:
        content = get_html(url)
        if not content:
            log(f"[WARN] No RSS content for {url}")
            return out
        feed = feedparser.parse(content)
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
            if dt < cutoff_utc: continue
            out.append({"title": title, "link": clean_url(link), "date": dt,
                        "source": urlparse(link).netloc or urlparse(url).netloc})
        log(f"[INFO] RSS {url} → {len(out)} items after cutoff")
        return out
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

def make_rss(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    build_comment = f"<!-- build {datetime.utcnow().isoformat()}Z -->"
    esc = lambda s: html.escape(s or "", quote=True)
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>','<rss version="2.0">','<channel>',
        f'  <title>Noticias {esc(country.title())}</title>','  <link>https://github.com/</link>',
        '  <description>Feed generado automáticamente</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>','  <generator>news-feeds GitHub Action</generator>',
        f'  {build_comment}',
    ]
    for it in items:
        parts += [
            '  <item>', f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
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

def generate_country_feed(country: str, urls: list[str], limit: int):
    log(f"[RUN] Generating {country} (limit {limit})")
    items, cutoff = [], datetime.utcnow() - timedelta(days=1)
    for url in urls:
        try:
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff)
            else:
                got = scrape_site(url, limit)
            items += got
            log(f"[INFO] Source done: {url} → {len(got)} items")
        except Exception:
            log(f"[ERROR] source {url} crashed:\n{traceback.format_exc()}")

    items.sort(key=lambda x: x["date"], reverse=True)

    uniq, final = set(), []
    for it in items:
        key = hashlib.md5((norm_text(it["title"])+"|"+urlparse(it["link"]).netloc).encode()).hexdigest()
        if key in uniq: continue
        uniq.add(key)
        final.append(it)
        if len(final) >= limit: break

    if not final:
        log(f"[WARN] {country}: no items collected. Writing placeholder.")
        final = [{
            "title": f"No se pudieron extraer titulares para {country} en este run",
            "link": "https://example.com/",
            "date": datetime.utcnow(),
            "source": "generator"
        }]

    write_feed(country, final)

def main():
    for country, urls in SOURCES.items():
        try:
            generate_country_feed(country, urls, LIMITS[country])
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")
            # aún así escribe placeholder para no romper el workflow
            write_feed(country, [{
                "title": f"Error generando {country}",
                "link": "https://example.com/",
                "date": datetime.utcnow(),
                "source": "generator"
            }])

if __name__ == "__main__":
    main()
