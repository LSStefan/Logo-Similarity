import pandas as pd
import os
from logo_extractor import download_logo
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing



df = pd.read_parquet('logos.snappy.parquet')
domains = df["domain"].dropna().astype(str).tolist()

def add_https(domain):
    d = domain.strip()
    if not d.startswith("http"):
        d = "https://" + d
    return d.rstrip("/")

# def domain_to_filename(domain):
#     return domain.replace(".", "_").replace("/", "_")

def worker(domain):
    url = add_https(domain)
    #fname = domain_to_filename(domain)
    try:
        download_logo(url)
        return True
    except Exception as e:
        print(f"[ERROR] {domain}: {e}")
        return False

# FOARTE MARE NUMAR DE THREAD-URI pentru IO-bound
MAX_WORKERS = multiprocessing.cpu_count() * 200   # poate fi ajustat

success = 0
fail = 0

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(worker, d): d for d in domains}
    for fut in as_completed(futures):
        if fut.result():
            success += 1
        else:
            fail += 1

total = success + fail
rate = (success / total) * 100 if total > 0 else 0

print("\n--- Rezultat final ---")
print(f"Total site-uri procesate: {total}")
print(f"Logo-uri descarcate cu succes: {success}")
print(f"Logo-uri nereusite: {fail}")
print(f"Rata de extragere: {rate:.2f}%")
