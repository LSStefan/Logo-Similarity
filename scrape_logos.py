import asyncio, aiohttp, aiofiles, os, csv, re, io, hashlib
import pandas as pd
from bs4 import BeautifulSoup, FeatureNotFound
from urllib.parse import urljoin, urlparse

# CONFIG
PARQUET = "logos.snappy.parquet"
COLUMN = None  
OUTDIR = "logos"
CSV = "mapping.csv"
CONCURRENCY_TOTAL = 200        # nr maxim task-uri simultan
CONNECTOR_LIMIT = 200          # nr conexiuni totale
CONNECTOR_PER_HOST = 10        # per host
TIMEOUT = 15
RETRIES = 2
HEADERS_HTML = {"User-Agent": "LogoScraper/2.0 (+contact@example.com)"}
HEADERS_IMG  = {"User-Agent": "LogoScraper/2.0 (+contact@example.com)", "Accept": "image/*"}
MIN_BYTES = 500                # minim bytes pt a considera imagine
# ----------------------------

os.makedirs(OUTDIR, exist_ok=True)

#util domenii
CAND_COLS = ["site","url","domain","hostname","website","home","homepage","site_url","siteDomain","host"]

def _to_https_host(s: str) -> str:
    s = (s or "").strip()
    if not s: return ""
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", s):
        host = urlparse(s).netloc or urlparse(s).path
    else:
        host = urlparse("https://" + s).netloc or urlparse("https://" + s).path
    host = host.strip("/").strip(".")
    if host.startswith("www."): host = host[4:]
    if not re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", host): return ""
    return "https://" + host

def load_sites_from_parquet(path: str, column: str | None):
    df = pd.read_parquet(path)
    if column and column in df.columns:
        series = df[column].astype(str)
    else:
        col = next((c for c in CAND_COLS if c in df.columns), None)
        if col:
            series = df[col].astype(str)
        else:
            text_cols = [c for c in df.columns if pd.api.types.is_string_dtype(df[c])]
            if not text_cols: return []
            series = pd.Series(pd.unique(pd.concat([df[c].astype(str) for c in text_cols])))
    sites = (series.dropna().map(_to_https_host).replace("", pd.NA).dropna().drop_duplicates())
    return sites.tolist()

#parsare HTML
def make_soup(html: str):
    try: return BeautifulSoup(html, "lxml")
    except FeatureNotFound: return BeautifulSoup(html, "html.parser")

def extract_candidates(html, base):
    soup = make_soup(html)
    cand = []
    for tag in soup.find_all(["meta","link"]):
        if tag.name == "meta":
            name = (tag.get("name","") or "").lower()
            prop = (tag.get("property","") or "").lower()
            if prop in ("og:image","og:image:url") or name in ("twitter:image","twitter:image:src"):
                if tag.get("content"): cand.append(urljoin(base, tag["content"]))
        elif tag.name == "link":
            rel = " ".join(tag.get("rel",[]) or []).lower()
            if "icon" in rel or "apple-touch-icon" in rel:
                if tag.get("href"): cand.append(urljoin(base, tag["href"]))
    for img in soup.find_all("img"):
        src = img.get("src")
        if not src: continue
        attrs = " ".join(filter(None, [
            img.get("id",""),
            " ".join(img.get("class",[]) if isinstance(img.get("class"), list) else [str(img.get("class") or "")]),
            img.get("alt","")
        ])).lower()
        if "logo" in attrs or "brand" in attrs or src.lower().endswith((".svg",".png",".jpg",".jpeg",".webp",".gif",".ico")):
            cand.append(urljoin(base, src))
    # uniq
    seen, out = set(), []
    for c in cand:
        if c not in seen:
            seen.add(c); out.append(c)
    return out

#retea
def guess_ext(data: bytes, content_type: str, url: str) -> str:
    ct = (content_type or "").lower()
    if "image/png" in ct: return "png"
    if "image/jpeg" in ct or "image/jpg" in ct: return "jpg"
    if "image/svg" in ct: return "svg"
    if "image/webp" in ct: return "webp"
    if "image/gif" in ct: return "gif"
    if "image/x-icon" in ct or "image/vnd.microsoft.icon" in ct: return "ico"
    # URL
    path = url.lower().split("?")[0]
    for ext in ("png","jpg","jpeg","svg","webp","gif","ico"):
        if path.endswith("."+ext): return "jpg" if ext=="jpeg" else ext
    # semnaturi
    if data.startswith(b"\x89PNG\r\n\x1a\n"): return "png"
    if data.startswith(b"\xff\xd8"): return "jpg"
    b2 = data[:16]
    if b2.startswith(b"<svg") or b"<svg" in data[:200].lower(): return "svg"
    if b2[:4] == b"RIFF" and b2[8:12] == b"WEBP": return "webp"
    if b2[:6] in (b"GIF87a", b"GIF89a"): return "gif"
    return "png"

class Fetcher:
    def __init__(self, session, sem):
        self.session = session
        self.sem = sem

    async def get_text(self, url, headers):
        for _ in range(RETRIES+1):
            try:
                async with self.sem:
                    async with self.session.get(url, headers=headers, timeout=TIMEOUT) as r:
                        if r.status == 200:
                            return await r.text()
            except Exception:
                pass
        return None

    async def get_bytes(self, url, headers):
        for _ in range(RETRIES+1):
            try:
                async with self.sem:
                    async with self.session.get(url, headers=headers, timeout=TIMEOUT) as r:
                        if r.status == 200:
                            data = await r.read()
                            if data: return data, r.headers.get("Content-Type","")
            except Exception:
                pass
        return None, ""

async def save_image_bytes(data, outdir, site_host, ext):
    h = hashlib.sha1(data).hexdigest()[:8]
    fname = f"{site_host.replace('://','_').replace('/','_')}_{h}.{ext}"
    path = os.path.join(outdir, fname)
    async with aiofiles.open(path, "wb") as f:
        await f.write(data)
    return path

async def download_first_valid(fetcher: Fetcher, site_norm: str, candidates: list, writer, site_label: str):
    # adauga favicon-uri standard in fata cozii
    base_favs = [
        urljoin(site_norm, "/favicon.ico"),
        urljoin(site_norm, "/apple-touch-icon.png"),
        urljoin(site_norm, "/apple-touch-icon-precomposed.png")
    ]
    # dedupe
    urls = []
    seen = set()
    for u in base_favs + candidates:
        if u not in seen:
            seen.add(u); urls.append(u)

    # lanseaza toate cererile de imagini in paralel si ia prima valida
    tasks = []
    for u in urls:
        tasks.append(asyncio.create_task(fetcher.get_bytes(u, HEADERS_IMG)))

    try:
        for t, u in zip(asyncio.as_completed(tasks), urls):
            data, ct = await t
            if not data or len(data) < MIN_BYTES: 
                continue
            # accepta doar daca e imagine (din header sau semnatura)
            if "image/" in (ct or "").lower() or data[:2] in (b"\x89P", b"\xff\xd8") or b"<svg" in data[:200].lower():
                ext = guess_ext(data, ct, u)
                path = await save_image_bytes(data, OUTDIR, urlparse(site_norm).netloc, ext)
                writer.writerow([site_label, u, path, ""])
                return True
        return False
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
                try: await task
                except: pass

async def process_site(fetcher: Fetcher, site: str, writer):
    # prefera HTTPS; fallback pe HTTP doar daca pagina nu merge
    site_norm = site if site.startswith(("http://","https://")) else "https://" + site
    html = await fetcher.get_text(site_norm, HEADERS_HTML)
    if html is None and site_norm.startswith("https://"):
        alt = "http://" + site_norm[len("https://"):]
        html = await fetcher.get_text(alt, HEADERS_HTML)
        if html: site_norm = alt
    if html is None:
        writer.writerow([site, "", "", "error_fetch"])
        return

    candidates = extract_candidates(html, site_norm)
    ok = await download_first_valid(fetcher, site_norm, candidates, writer, site)
    if not ok:
        writer.writerow([site, "", "", "no_logo_found"])

async def main():
    sites = load_sites_from_parquet(PARQUET, COLUMN)
    if not sites: raise SystemExit("Nu am gasit niciun site valid in parquet.")
    print(f"Am gasit {len(sites)} site-uri unice.")

    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, limit_per_host=CONNECTOR_PER_HOST, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=None, connect=TIMEOUT)
    sem = asyncio.Semaphore(CONCURRENCY_TOTAL)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        fetcher = Fetcher(session, sem)
        with open(CSV, "w", newline="", encoding="utf-8") as csvf:
            writer = csv.writer(csvf)
            writer.writerow(["site","source_url","saved_path","error"])
            tasks = [process_site(fetcher, s, writer) for s in sites]
            # ruleaza in valuri pentru memorie stabila
            BATCH = 2000
            for i in range(0, len(tasks), BATCH):
                await asyncio.gather(*tasks[i:i+BATCH], return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
