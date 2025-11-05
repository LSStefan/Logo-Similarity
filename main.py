import pandas as pd
import requests

#Citeste fisierul
df = pd.read_parquet("logos.snappy.parquet")


#Afisare primele randuri
print(df.head())
print(df.info())


def check_site(url):
    try:
        response = requests.get(url, timeout=5)
        return response.status_code
    except:
        return None



def add_https(domain):
    domain = domain.strip()
    if not domain.startswith("http"):
        return "https://" + domain
    return domain

df["url"] = df["domain"].apply(add_https)
print(df.head())
