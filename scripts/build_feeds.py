# scripts/build_feeds.py
# Scraping-only feeds (VE/PA/DO) — SIN Bloomberg/Bloomberg Línea — NO toca workflow.
# Objetivo: llenar LIMITS[pais] con mezcla justa y, si falta material, elevar cap por dominio hasta completar.

import os, re, html, hashlib, traceback, random
from datetime import datetime
from urllib.parse import urlparse, urljoin, urlunparse, parse_qsl, urlencode
from time import sleep

import requests
from bs4 import BeautifulSoup

OUTPUT_DIR = "data"
REQUEST_TIMEOUT = 25
RETRIES = 2
SLEEP_BETWEEN = 0.8
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AngieNewsBot/2.2",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
}

# === TUS FUENTES (sin RSS) ===
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
        "https://listindiario.com/",
        "https://www.elcaribe.com.do/",
        "https://eldinero.com.do/",
    ],
}

# Límite final por país (número de items en el XML)
LIMITS = {"venezuela": 10, "panama": 10, "dominicana": 10}

# Cap inicial por dominio (se eleva si falta material para llegar al límite)
START_CAP_PER_DOMAIN = 2

# Overfetch: cuántos candidatos intentamos exprimir por portada
MAX_LINKS_PER_SOURCE = 80

# Filtro mínimo de longitud de titular (relajado para no quedarnos cortos)
MIN_TITLE_LEN = 12

# Bloqueo de dominios no deseados
BLOCKED_DOMAINS = {"bloomberg.com", "bloomberglinea.com", "www.bloomberglinea.com", "www.bloomberg.com"}

# Selectores por dominio (agresivos) + fallback genérico
SITE_SELECTORS = {
    # Venezuela
    "www.elnacional.com": [
        "article h2 a", "article h3 a", ".headline a", ".post-title a",
        "h1 a[href*='/']", "h2 a[href*='/']", "h3 a[href*='/']",
    ],
    "talcualdigital.com": [
        "h2.entry-title a", "article h2 a", "div.post-title h2 a",
        ".jeg_post_title a", ".post-title a", "h3 a[href*='/']",
    ],
    "efectococuyo.com": [
        "article h2 a", "h2 a[href*='/']", "h3 a[href*='/']", ".jeg_post_title a",
        ".post-title a",
    ],
    # Panamá
    "www.prensa.com": [
        "article h2 a[href*='prensa.com']", "h1 a[href*='prensa.com']",
        "h2 a[href*='prensa.com']", ".headline a[href*='prensa.com']",
        "a[href*='/economia/']", "a[href*='/sociedad/']", "a[href*='/panorama/']",
    ],
    "www.laestrella.com.pa": [
        "article h2 a[href*='laestrella.com.pa']", "h2 a[href*='laestrella.com.pa']",
        "h1 a[href*='laestrella.com.pa']", ".headline a[href*='laestrella.com.pa']",
        "a[href*='/panama/']", "a[href*='/economia/']", "a[href*='/vida-y-cultura/']",
    ],
    # Dominicana
    "listindiario.com": [
        "article h2 a", "h2 a[href*='listindiario.com']", "h3 a[href*='listindiario.com']",
        ".post-title a", ".headline a",
    ],
    "www.elcaribe.com.do": [
        "article h2 a", "h2 a[href*='elcaribe.com.do']", "a.post-title[href]",
        ".post-title a", ".headline a",
    ],
    "eldinero.com.do": [
        "article h2 a", "h2 a[href*='eldinero.com.do']", "a.post-title[href]",
        ".post-title a", ".headline a",
    ],
}

# ---------------- utilidades ----------------
def log(msg): print(msg, flush=True)

def strip_www(host: str) -> str:
    h = (host or "").strip().lower()
    return h[4:] if h.startswith("www.") else h

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

def norm_title(s: str) -> str:
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
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            if not r.encoding or r.encoding.lower() == "iso-8859-1":
                r.encoding = r.apparent_encoding or "utf-8"
            return r
        except Exception as e:
            log(f"[WARN] GET fail ({i+1}/{RETRIES+1}) {url}: {e}")
            sleep(SLEEP_BETWEEN)
    return None

def to_soup(resp):
    if not resp: return None
    try:
        return BeautifulSoup(resp.text, "html.parser")
    except Exception:
        try:
            return BeautifulSoup(resp.text, "lxml")
        except Exception:
            return None

def looks_like_article(link_url: str, host: str) -> bool:
    """
    Regla relajada: solo misma web; evita tag/autor/categoría; requiere al menos 1 segmento de slug.
    (No bloqueamos secciones; muchas home insertan artículos en /categoria/ pero con slug adicional.)
    """
    p = urlparse(link_url); h = strip_www(p.netloc); hp = strip_www(host)
    if h != hp:
        return False  # NO externas (tú no las quieres)
    path = p.path.lower()
    if path in {"/", "", "/home"}: return False
    banned = ("/tag/", "/etiqueta/", "/autor", "/author")
    if any(seg in path for seg in banned): return False
    # requiere algo tipo /seccion/slug o /yyyy/mm/dd/slug
    segments = [s for s in path.strip("/").split("/") if s]
    return len(segments) >= 2

# ---------------- scraping ----------------
def scrape_home(url: str, limit_candidates: int) -> list[dict]:
    out = []
    resp = get_response(url)
    if not resp:
        log(f"[WARN] No HTML for {url}")
        return out
    soup = to_soup(resp)
    if not soup:
        log(f"[WARN] No soup for {url}")
        return out

    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}/"
    host = parsed.netloc.lower()
    selectors = SITE_SELECTORS.get(host, []) + [
        "article a[href]", "h1 a[href]", "h2 a[href]", "h3 a[href]",
        ".headline a[href]", ".post-title a[href]"
    ]

    seen, total = set(), 0
    for sel in selectors:
        hits = 0
        for tag in soup.select(sel):
            href = tag.get("href")
            title = tag.get_text(strip=True)
            if not href or not title: continue
            full = clean_url(abs_url(base, href))
            if not looks_like_article(full, host): continue
            k = url_key(full)
            if k in seen: continue
            seen.add(k)

            dom = strip_www(urlparse(full).netloc)
            if dom in BLOCKED_DOMAINS: continue
            if len(title) < MIN_TITLE_LEN: continue

            out.append({
                "title": title,
                "link": full,
                "date": datetime.utcnow(),
                "domain": dom,
                "urlk": k,
            })
            hits += 1; total += 1
            if total >= limit_candidates: break
        log(f"[INFO] {host} selector '{sel}' → {hits}")
        if total >= limit_candidates: break

    # dedupe interno por URL + (titulo+dominio)
    uniq_url, uniq_td, dedup = set(), set(), []
    for it in out:
        kurl = it["urlk"]
        ktd = hashlib.md5((norm_title(it["title"])+"|"+it["domain"]).encode()).hexdigest()
        if kurl in uniq_url or ktd in uniq_td: continue
        uniq_url.add(kurl); uniq_td.add(ktd); dedup.append(it)
    return dedup

# ---------------- mezcla con cap dinámico ----------------
def mix_with_dynamic_cap(items: list[dict], limit_total: int, start_cap: int) -> list[dict]:
    # dedupe global
    seen_url, seen_td, pool = set(), set(), []
    for it in items:
        kurl = it.get("urlk") or url_key(it["link"])
        ktd = hashlib.md5((norm_title(it["title"])+"|"+it["domain"]).encode()).hexdigest()
        if kurl in seen_url or ktd in seen_td: continue
        seen_url.add(kurl); seen_td.add(ktd); pool.append(it)

    # buckets por dominio
    buckets = {}
    for it in pool:
        d = strip_www(it["domain"])
        if d in BLOCKED_DOMAINS: continue
        buckets.setdefault(d, []).append(it)
    for d in buckets:
        buckets[d].sort(key=lambda x: x["date"], reverse=True)

    cap = start_cap
    while cap <= max(limit_total, start_cap):
        snapshot = {d: list(arr) for d, arr in buckets.items()}
        domains = list(snapshot.keys())
        random.shuffle(domains)
        picked, counts = [], {d:0 for d in domains}
        i = 0
        while len(picked) < limit_total and domains:
            d = domains[i % len(domains)]
            if snapshot[d] and counts[d] < cap:
                picked.append(snapshot[d].pop(0)); counts[d] += 1
            if not snapshot[d] or counts[d] >= cap:
                domains.pop(i % len(domains))
            else:
                i += 1
        if len(picked) >= limit_total:
            # recorte defensivo por cap
            final, cnt = [], {}
            for it in picked:
                d = strip_www(it["domain"])
                if cnt.get(d,0) >= cap: continue
                final.append(it); cnt[d] = cnt.get(d,0) + 1
                if len(final) >= limit_total: break
            return final[:limit_total]
        cap += 1

    # si aún no llenamos (muy raro), devuelve lo que haya
    return picked[:limit_total] if 'picked' in locals() else []

# ---------------- writer ----------------
def make_rss(country: str, items: list[dict]) -> str:
    now_http = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    esc = lambda s: html.escape(s or "", quote=True)
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>', '<rss version="2.0">', '<channel>',
        f'  <title>Noticias {esc(country.title())}</title>',
        '  <link>https://github.com/</link>',
        '  <description>Feed generado automáticamente</description>',
        f'  <lastBuildDate>{now_http}</lastBuildDate>',
        '  <generator>news-feeds GitHub Action</generator>',
        f'  <!-- build {datetime.utcnow().isoformat()}Z -->',
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

# ---------------- pipeline ----------------
def generate_country_feed(country: str, urls: list[str], limit_total: int):
    log(f"[RUN] {country} | limit={limit_total} | start_cap={START_CAP_PER_DOMAIN} | scraping-only")
    all_items = []
    for u in urls:
        try:
            got = scrape_home(u, MAX_LINKS_PER_SOURCE)
            all_items.extend(got)
            log(f"[INFO] {country}: {u} → {len(got)} items")
        except Exception:
            log(f"[ERROR] {country} source {u} crashed:\n{traceback.format_exc()}")

    # orden aproximado por “recencia” (sin timestamp real)
    all_items.sort(key=lambda x: x["date"], reverse=True)
    mixed = mix_with_dynamic_cap(all_items, limit_total=limit_total, start_cap=START_CAP_PER_DOMAIN)

    # SIEMPRE escribimos (garantiza diff en XML por timestamp del canal)
    write_feed(country, mixed)

def main():
    random.seed()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for country, urls in SOURCES.items():
        try:
            generate_country_feed(country, urls, LIMITS[country])
        except Exception:
            log(f"[FATAL] {country} crashed:\n{traceback.format_exc()}")
            # incluso así, escribe un feed vacío (pero con timestamp) para que haya diff
            with open(os.path.join(OUTPUT_DIR, f"{country}.xml"), "w", encoding="utf-8") as f:
                f.write(make_rss(country, []))

if __name__ == "__main__":
    main()
