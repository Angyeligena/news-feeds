# scripts/build_feeds.py
# Feeds diarios PA/VE/DO SIN Google News y SIN Bloomberg/Bloomberg Línea.

import os, re, html, hashlib, traceback, random
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests, feedparser
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AngieNewsBot/1.4; +https://github.com/)"}

# === FUENTES  ===
SOURCES = {
    "venezuela": [
        "https://www.elnacional.com/",
        "https://talcualdigital.com/",
        "https://efectococuyo.com/",
        # (eliminado) "https://www.bloomberglinea.com/latinoamerica/venezuela/",
    ],
    "panama": [
        "https://www.prensa.com/",
        "https://www.laestrella.com.pa/",
        # (eliminado) "https://www.bloomberglinea.com/latinoamerica/panama/",
    ],
    "dominicana": [
        "https://www.diariolibre.com/rss/portada.xml",
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

# LÍMITES
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}
HARD_MAX_PER_DOMAIN = 2              # diversidad por defecto (intentado primero)
FALLBACK_WINDOW_HOURS = 48           # ampliar ventana si faltan piezas
MIN_TITLE_LEN = 20                   # filtro de calidad


SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": ["article h2 a", "h3 a", ".headline a", "h2 a[href]"],
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
        return urlunparse((p.scheme or "https", p.netloc.lower(), p.path.rstrip("/"), "", urlencode(qs), ""))
    except Exception:
        return u

def url_key(u: str) -> str:
    try:
        p = urlparse(clean_url(u))
        return f"{strip_www(p.netloc)}{p.path}"
    except Exception:
        return u

def norm_text(s: str) -> str:
    s = (s or "")
    s = re.sub(r"\b(LIVE|UPDATE|BREAKING|EN VIVO|ÚLTIMA HORA|ACTUALIZACIÓN)\b[:\-–]*\s*", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip().lower()

def abs_url(base: str, href: str) -> str:
    if not href: return ""
    if href.startswith("//"): return "https:" + href
    return urljoin(base, href)

def get_response(url: str):
    for i in range(RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(1.2)
    return None

def fetch_html(url: str) -> BeautifulSoup | None:
    r = get_response(url)
    return BeautifulSoup(r.text, "html.parser") if r else None

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

        seen_urlk = set()
        for sel in selectors:
            hits = 0
            for tag in soup.select(sel):
                link = tag.get("href")
                text = tag.get_text(strip=True)
                if not link:
                    a = tag.find("a", href=True)
                    if a:
                        link = a["href"]
                        text = text or a.get_text(strip=True)
                if not text or not link:
                    continue
                full = abs_url(base, link)
                k = url_key(full)
                if k in seen_urlk:
                    continue
                seen_urlk.add(k)
                dom = strip_www(urlparse(full).netloc)
                if dom in BLOCKED_DOMAINS:
                    continue  # BLOOMBERG FUERA

                out.append({
                    "title": text,
                    "link": clean_url(full),
                    "date": datetime.utcnow(),
                    "domain": dom,
                    "urlk": k,
                })
                hits += 1
                if len(out) >= soft_limit * 4: break
            log(f"[INFO] {host_for_selectors} selector '{sel}' → {hits}")
            if len(out) >= soft_limit * 4: break

        # filtro de calidad
        before = len(out)
        out = [it for it in out if len(it["title"]) >= MIN_TITLE_LEN]
        log(f"[INFO] {host_for_selectors} filter >={MIN_TITLE_LEN} chars: {before} → {len(out)}")

        # dedupe por URL y por (titulo+dominio)
        uniq_url, uniq_td, dedup = set(), set(), []
        for it in out:
            kurl = it["urlk"]
            ktd = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
            if kurl in uniq_url or ktd in uniq_td: continue
            uniq_url.add(kurl); uniq_td.add(ktd); dedup.append(it)
        return dedup[: soft_limit * 2]
    except Exception:
        log(f"[ERROR] scrape_site({url}) crashed:\n{traceback.format_exc()}")
        return out

def fetch_rss(url: str, cutoff_utc: datetime, soft_limit: int, allow_widen=False) -> list[dict]:
    out: list[dict] = []
    try:
        r = get_response(url)
        if not r:
            log(f"[WARN] No RSS for {url}")
            return out
        feed = feedparser.parse(r.content)
        # define cutoff (24h normal; 48h si widen=True)
        cutoff = cutoff_utc if not allow_widen else datetime.utcnow() - timedelta(hours=FALLBACK_WINDOW_HOURS)
        tmp = []
        for e in feed.entries[: soft_limit * 8]:
            title = getattr(e,"title","") or ""
            link  = getattr(e,"link","") or ""
            if not title or not link: continue
            if getattr(e,"published_parsed", None):
                dt = datetime(*e.published_parsed[:6])
            elif getattr(e,"updated_parsed", None):
                dt = datetime(*e.updated_parsed[:6])
            else:
                dt = datetime.utcnow()
            if dt < cutoff: continue
            dom = strip_www(urlparse(link).netloc or urlparse(url).netloc)
            if dom in BLOCKED_DOMAINS: continue
            tmp.append({
                "title": title,
                "link": clean_url(link),
                "date": dt,
                "domain": dom,
                "urlk": url_key(link),
            })
        log(f"[INFO] RSS {url} → {len(tmp)} after cutoff({24 if not allow_widen else FALLBACK_WINDOW_HOURS}h)")

        # dedupe y limitar
        uniq_url, uniq_td, dedup = set(), set(), []
        for it in sorted(tmp, key=lambda x: x["date"], reverse=True):
            kurl = it["urlk"]
            ktd = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
            if kurl in uniq_url or ktd in uniq_td: continue
            uniq_url.add(kurl); uniq_td.add(ktd); dedup.append(it)
        return dedup[: soft_limit * 3]
    except Exception:
        log(f"[ERROR] fetch_rss({url}) crashed:\n{traceback.format_exc()}")
        return out

# ----------------- mezcla justa con CAP dinámico -----------------
def mix_with_cap(items: list[dict], limit_total: int, start_cap: int) -> list[dict]:
    """
    1) Dedupe global por URL y (titulo+dominio)
    2) Buckets por dominio
    3) Intentar round-robin con cap = start_cap; si no alcanza, subir cap (start_cap+1, +2, ...)
    """
    # dedupe global
    seen_url, seen_td, pool = set(), set(), []
    for it in sorted(items, key=lambda x: x["date"], reverse=True):
        kurl = it.get("urlk") or url_key(it["link"])
        ktd = hashlib.md5((norm_text(it["title"])+"|"+it["domain"]).encode()).hexdigest()
        if kurl in seen_url or ktd in seen_td: continue
        seen_url.add(kurl); seen_td.add(ktd); pool.append(it)

    # buckets
    buckets = {}
    for it in pool:
        d = strip_www(it["domain"])
        if d in BLOCKED_DOMAINS:  # seguridad extra
            continue
        buckets.setdefault(d, []).append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: x["date"], reverse=True)

    def round_robin(cap: int) -> list[dict]:
        domains = list(buckets.keys())
        random.shuffle(domains)
        picked, counts = [], {d:0 for d in domains}
        i = 0
        while len(picked) < limit_total and domains:
            d = domains[i % len(domains)]
            if buckets[d] and counts[d] < cap:
                picked.append(buckets[d].pop(0))
                counts[d] += 1
            if not buckets[d] or counts[d] >= cap:
                domains.pop(i % len(domains))
            else:
                i += 1
        # backfill respetando cap
        if len(picked) < limit_total:
            rest = []
            for d, arr in buckets.items():
                rest.extend(arr)
            rest.sort(key=lambda x: x["date"], reverse=True)
            for it in rest:
                d = strip_www(it["domain"])
                if counts.get(d,0) >= cap: continue
                picked.append(it)
                counts[d] = counts.get(d,0) + 1
                if len(picked) >= limit_total: break
        return picked[:limit_total]

    cap = start_cap
    while True:
        # clona buckets (no mutar original al iterar caps)
        snapshot = {d:list(arr) for d,arr in buckets.items()}
        picked = []
        domains = list(snapshot.keys())
        random.shuffle(domains)
        counts = {d:0 for d in domains}
        i = 0
        while len(picked) < limit_total and domains:
            d = domains[i % len(domains)]
            if snapshot[d] and counts[d] < cap:
                picked.append(snapshot[d].pop(0))
                counts[d] += 1
            if not snapshot[d] or counts[d] >= cap:
                domains.pop(i % len(domains))
            else:
                i += 1
        # backfill
        if len(picked) < limit_total:
            rest = []
            for d, arr in snapshot.items():
                rest.extend(arr)
            rest.sort(key=lambda x: x["date"], reverse=True)
            for it in rest:
                d = strip_www(it["domain"])
                if counts.get(d,0) >= cap: continue
                picked.append(it)
                counts[d] = counts.get(d,0) + 1
                if len(picked) >= limit_total: break

        if len(picked) >= limit_total or cap >= limit_total:
            # recorte final defensivo por cap
            final, cnt = [], {}
            for it in picked:
                d = strip_www(it["domain"])
                if cnt.get(d,0) >= cap: continue
                final.append(it); cnt[d] = cnt.get(d,0) + 1
                if len(final) >= limit_total: break
            return final[:limit_total]
        else:
            cap += 1  # sube cap para poder completar

# ----------------- writer -----------------
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
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(make_rss(country, items))
    log(f"[OK] Wrote {path} ({len(items)} items)")

# ----------------- pipeline por país -----------------
def collect_items(urls: list[str], soft_limit: int, widen: bool=False) -> list[dict]:
    cutoff_24h = datetime.utcnow() - timedelta(hours=24)
    items = []
    for url in urls:
        try:
            if url.endswith(".xml") or "rss" in url.lower() or "feed" in url.lower():
                got = fetch_rss(url, cutoff_utc=cutoff_24h, soft_limit=soft_limit, allow_widen=widen)
            else:
                got = scrape_site(url, soft_limit=soft_limit)
            items += got
            log(f"[INFO] source {url} → {len(got)} items")
        except Exception:
            log(f"[ERROR] source {url} crashed:\n{traceback.format_exc()}")
    # quitar cualquier residuo bloqueado (seguridad extra)
    items = [it for it in items if strip_www(urlparse(it["link"]).netloc) not in BLOCKED_DOMAINS]
    return items

def generate_country_feed(country: str, urls: list[str], limit_total: int):
    log(f"[RUN] {country} - limit {limit_total}, cap {HARD_MAX_PER_DOMAIN}, no Bloomberg")
    soft_limit = max(limit_total, 12)

    # 1) recolecta 24h
    items = collect_items(urls, soft_limit=soft_limit, widen=False)

    # 2) si faltan piezas, intenta con ventana ampliada 48h (solo RSS respeta ventana)
    if len(items) < limit_total:
        log(f"[INFO] {country} shortage {len(items)}/{limit_total} → widen window")
        items += collect_items([u for u in urls if u.endswith(".xml") or "rss" in u.lower() or "feed" in u.lower()],
                               soft_limit=soft_limit, widen=True)

    # 3) mezcla con cap dinámico (empieza en 2 y sube si falta)
    mixed = mix_with_cap(items, limit_total=limit_total, start_cap=HARD_MAX_PER_DOMAIN)

    # 4) si aún no alcanza, mete placeholder(s) para no romper el feed
    if len(mixed) < limit_total:
        log(f"[WARN] {country} final shortage {len(mixed)}/{limit_total} → placeholders")
        while len(mixed) < limit_total:
            mixed.append({
                "title": f"No se encontraron más titulares recientes para {country}",
                "link": "https://example.com/",
                "date": datetime.utcnow(),
                "domain": "generator",
                "urlk": "generator",
            })

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
                "urlk": "generator",
            }])

if __name__ == "__main__":
    main()
