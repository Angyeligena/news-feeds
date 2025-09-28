# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News.
# FIXES CLAVE:
# - Dominio de la noticia = dominio del ENLACE (normalizado), no de la portada.
# - Round-robin con tope por dominio + verificación final (cap duro).
# - <category> usa dominio normalizado.

import os, re, html, hashlib, traceback, random
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.2; +https://github.com/)"}

# === FUENTES (tus links) ===
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",
        "https://talcualdigital.com/",
        "https://efectococuyo.com/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/",
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada.xml",
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

# Límite total por país
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# Tope por dominio dentro del feed final
MAX_PER_DOMAIN = 4  # puedes bajarlo a 3 si quieres más diversidad

SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a"],
    "talcualdigital.com": ["h2.entry-title a", "article h2 a", "div.post-title h2 a"],
    "efectococuyo.com": ["article h2 a", "h2 a[href*='/']"],
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

# ----------------- utilidades -----------------
def log(msg: str): print(msg, flush=True)

def strip_www(host: str) -> str:
    host = (host or "").strip().lower()
    return host[4:] if host.startswith("www.") else host

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
            return r.content
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    c = get_html(url)
    return BeautifulSoup(c, "html.parser") if c else None

# ----------------- scraping / rss -----------------
def scrape_site(url: str, soft_limit: int) -> list[dict]:
    out: list[dict] = []
    try:
        soup = fetch_html(url)
        if not soup:
            log(f"[WARN] No HTML for {url}")
            return out
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}/"
        host_for_selectors = parsed.netloc.lower()
        selectors = SITE_SELECTORS.get(host_for_selectors, []) + ["h1 a[href]", "h2 a[href]", "article h2 a"]

        seen_links = set()
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
                if full in seen_links: continue
                seen_links.add(full)

                link_host = strip_www(urlparse(full).netloc)
                out.append({
                    "title": text,
                    "link": clean_url(full),
                    "date": datetime.utcnow(),
                    "domain": link_host,   # <-- dominio del ENLACE, NO de la portada
                })
                hits += 1
                if len(out) >= soft_limit * 4: break
            log(f"[INFO] {host_for_selectors} selector '{sel}' → {hits}")
            if len(out) >= soft_limit * 4: break

        # filtro de calidad
        before = len(out)
        out = [it for it in out if len(it["title"]) >= 25]
        log(f"[INFO] {host_for_selectors} filter >=25 chars: {before} → {len(out)}")

        # fallback si vacío
        if not out:
            fb = 0
            for a in soup.select("a[href]"):
                href = a.get("href",""); txt=a.get_text(strip=True)
                full = abs_url(base, href)
                link_host = strip_www(urlparse(full).netloc)
                if link_host != strip_www(host_for_selectors) or len(txt) < 35: 
                    continue
                out.append({
                    "title": txt,
                    "link": clean_url(full),
                    "date": datetime.utcnow(),
                    "domain": link_host,
                })
                fb += 1
                if len(out) >= soft_limit * 2: break
            log(f"[INFO] {host_for_selectors} fallback anchors → {fb}")

        # dedupe por (titulo+dominio)
        uniq, dedup = set(), []
        for it in out:
            key = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
            if key in uniq: continue
            uniq.add(key)
            dedup.append(it)
        return dedup[: soft_limit * 2]
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

def fetch_rss(url: str, cutoff_utc: datetime, soft_limit: int) -> list[dict]:
    out: list[dict] = []
    try:
        c = get_html(url)
        if not c:
            log(f"[WARN] No RSS for {url}")
            return out
        feed = feedparser.parse(c)
        for e in feed.entries[: soft_limit * 6]:  # overfetch
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
            dom = strip_www(urlparse(link).netloc or urlparse(url).netloc)
            out.append({
                "title": title,
                "link": clean_url(link),
                "date": dt,
                "domain": dom,
            })
        log(f"[INFO] RSS {url} → {len(out)} after cutoff")
        return out
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

# ----------------- mezcla justa con CAP -----------------
def mix_by_domain(items: list[dict], limit_total: int, max_per_domain: int) -> list[dict]:
    """
    1) Dedupe global por (titulo normalizado + dominio)
    2) Bucket por dominio
    3) Round-robin entre dominios respetando max_per_domain
    4) Verificación final de cap (por si acaso)
    """
    # 1) dedupe global
    seen, pool = set(), []
    for it in sorted(items, key=lambda x: x["date"], reverse=True):
        key = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
        if key in seen: continue
        seen.add(key)
        pool.append(it)

    # 2) buckets por dominio
    buckets = {}
    for it in pool:
        d = strip_www(it["domain"])
        buckets.setdefault(d, []).append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: x["date"], reverse=True)

    # 3) round-robin
    domains = list(buckets.keys())
    random.shuffle(domains)
    picked = []
    counts = {d: 0 for d in domains}
    i = 0
    while len(picked) < limit_total and domains:
        d = domains[i % len(domains)]
        if buckets[d] and counts[d] < max_per_domain:
            picked.append(buckets[d].pop(0))
            counts[d] += 1
        if not buckets[d] or counts[d] >= max_per_domain:
            domains.pop(i % len(domains))
        else:
            i += 1

    # 4) backfill (sin romper el cap)
    if len(picked) < limit_total:
        # candidatos restantes
        rest = []
        for d, arr in buckets.items():
            rest.extend(arr)
        rest.sort(key=lambda x: x["date"], reverse=True)
        for it in rest:
            d = strip_www(it["domain"])
            if counts.get(d,0) >= max_per_domain:
                continue
            picked.append(it)
            counts[d] = counts.get(d,0) + 1
            if len(picked) >= limit_total:
                break

    # 5) verificación cap duro
    final, cnt = [], {}
    for it in picked:
        d = strip_www(it["domain"])
        if cnt.get(d,0) >= max_per_domain:
            continue
        final.append(it)
        cnt[d] = cnt.get(d,0) + 1
        if len(final) >= limit_total:
            break

    return final[:limit_total]

# ----------------- RSS writer -----------------
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
        dom = strip_www(it["domain"])
        parts += [
            '  <item>',
            f"    <title>{esc(it['title'])}</title>",
            f"    <link>{esc(it['link'])}</link>",
            f"    <pubDate>{it['date'].strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>",
            f"    <category>{esc(dom)}</category>",
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

# ----------------- pipeline por país -----------------
def generate_country_feed(country: str, urls: list[str], limit_total: int):
    log(f"[RUN] Generating {country} (limit {limit_total}, max/domain {MAX_PER_DOMAIN})")
    items, cutoff = [], datetime.utcnow() - timedelta(days=1)

    for url in urls:
        try:
            soft_limit = max(limit_total, 10)
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff, soft_limit)
            else:
                got = scrape_site(url, soft_limit)
            items += got
            log(f"[INFO] {country} source {url} → {len(got)} items")
        except Exception:
            log(f"[ERROR] {country} source {url} crashed:\n{traceback.format_exc()}")

    if not items:
        log(f"[WARN] {country}: no items at all → placeholder")
        items = [{
            "title": f"No se pudieron extraer titulares para {country} en este run",
            "link": "https://example.com/",
            "date": datetime.utcnow(),
            "domain": "generator",
        }]

    mixed = mix_by_domain(items, limit_total, MAX_PER_DOMAIN)

    # ASSERT final: nadie supera cap
    final_counts = {}
    for it in mixed:
        d = strip_www(it["domain"])
        final_counts[d] = final_counts.get(d,0) + 1
    offenders = [f"{d}:{c}" for d,c in final_counts.items() if c > MAX_PER_DOMAIN]
    if offenders:
        log(f"[WARN] cap offenders (shouldn't happen): {', '.join(offenders)}")
        # recorta si algo se coló (defensivo)
        trimmed, seen_cnt = [], {}
        for it in mixed:
            d = strip_www(it["domain"])
            if seen_cnt.get(d,0) >= MAX_PER_DOMAIN:
                continue
            trimmed.append(it)
            seen_cnt[d] = seen_cnt.get(d,0) + 1
            if len(trimmed) >= limit_total:
                break
        mixed = trimmed

    write_feed(country, mixed[:limit_total])

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
                "domain": "generator",
            }])

if __name__ == "__main__":
    main()
