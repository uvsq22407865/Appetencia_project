import os
import re
import json
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# --------- CONFIG ----------
CSV_PATH = r"data/metiers/5fa5949243f97 (1).csv"   # <-- adapte au nom exact chez toi
OUT_JSONL = "output/metiers_corpus.jsonl"
OUT_CSV = "output/metiers_corpus.csv"
CACHE_DIR = "output/cache_html"

SLEEP_SECONDS = 0.6     # pause entre requêtes (respect site)
TIMEOUT = 20
MAX_METIERS = 10     # None = tous, ou mets 50 pour tester

# User-Agent propre (important)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36"
}


def safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-\.]+", "_", s.strip())
    return s[:120]


def load_index(csv_path: str) -> pd.DataFrame:
    # Essaye ; si jamais ton CSV est en ; (fréquent en France), pandas détecte mal -> on force
    try:
        df = pd.read_csv(csv_path, sep=None, engine="python")
    except Exception:
        df = pd.read_csv(csv_path, sep=";", engine="python")

    # Normalise noms de colonnes
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def pick_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    On cherche les colonnes les plus probables.
    Ton CSV Onisep contient souvent:
    - libellé métier
    - code rome
    - lien site onisep.fr
    - domaine / sous-domaine
    """
    def find_col(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    col_metier = find_col(["libellé métier", "libelle metier", "libellé", "metier", "métier"])
    col_rome = find_col(["code rome", "rome"])
    col_url = find_col(["lien site onisep.fr", "lien site onisep", "lien onisep", "onisep", "url onisep"])
    col_dom = find_col(["domaine", "domaine/sous-domaine", "domaine sous-domaine", "domaine/sous domaine"])
    col_sdom = find_col(["sous-domaine", "sous domaine", "sousdomaine"])

    # Certaines colonnes peuvent ne pas exister, on gère
    keep = {
        "metier_onisep": col_metier,
        "code_rome": col_rome,
        "url_onisep": col_url,
        "domaine": col_dom,
        "sous_domaine": col_sdom
    }

    for k, v in keep.items():
        if v is None:
            df[k] = None
        else:
            df[k] = df[v]

    out = df[["metier_onisep", "code_rome", "url_onisep", "domaine", "sous_domaine"]].copy()
    out = out.dropna(subset=["url_onisep"])  # on a besoin d’une URL
    out["metier_onisep"] = out["metier_onisep"].fillna("").astype(str)
    out["code_rome"] = out["code_rome"].fillna("").astype(str)
    out["domaine"] = out["domaine"].fillna("").astype(str)
    out["sous_domaine"] = out["sous_domaine"].fillna("").astype(str)

    # Supprime doublons URL
    out = out.drop_duplicates(subset=["url_onisep"]).reset_index(drop=True)
    return out


def fetch_html(url: str, cache_path: str) -> str | None:
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            return f.read()

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        html = r.text
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html
    except Exception:
        return None


def extract_text_onisep(html: str) -> str:
    """
    Extraction 'simple mais robuste' :
    - supprime scripts/styles
    - récupère le texte principal
    - nettoie espaces
    """
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Heuristique: Onisep a souvent un contenu principal; sinon on prend le body
    main = soup.find("main")
    if main is None:
        main = soup.body if soup.body else soup

    text = main.get_text(separator="\n", strip=True)

    # nettoie lignes vides
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]
    text = "\n".join(lines)

    # limite de sécurité (évite méga pages)
    return text[:20000]


def build_corpus():
    os.makedirs("output", exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    df = load_index(CSV_PATH)
    df = pick_columns(df)

    if MAX_METIERS:
        df = df.head(MAX_METIERS)

    records = []
    with open(OUT_JSONL, "w", encoding="utf-8") as out:
        for _, row in tqdm(df.iterrows(), total=len(df)):
            metier = row["metier_onisep"]
            rome = row["code_rome"]
            url = row["url_onisep"]
            dom = row["domaine"]
            sdom = row["sous_domaine"]

            cache_name = safe_filename(f"{rome}_{metier}") + ".html"
            cache_path = os.path.join(CACHE_DIR, cache_name)

            html = fetch_html(url, cache_path)
            if not html:
                rec = {
                    "metier_onisep": metier,
                    "code_rome": rome,
                    "domaine": dom,
                    "sous_domaine": sdom,
                    "url_onisep": url,
                    "texte": "",
                    "status": "fetch_failed",
                }
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records.append(rec)
                time.sleep(SLEEP_SECONDS)
                continue

            texte = extract_text_onisep(html)
            status = "ok" if texte else "empty_text"

            rec = {
                "metier_onisep": metier,
                "code_rome": rome,
                "domaine": dom,
                "sous_domaine": sdom,
                "url_onisep": url,
                "texte": texte,
                "status": status,
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            records.append(rec)

            time.sleep(SLEEP_SECONDS)

    # CSV aussi (pratique)
    pd.DataFrame(records).to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("✅ Corpus généré:")
    print(" -", OUT_JSONL)
    print(" -", OUT_CSV)

    # petit bilan
    ok = sum(1 for r in records if r["status"] == "ok")
    failed = sum(1 for r in records if r["status"] == "fetch_failed")
    empty = sum(1 for r in records if r["status"] == "empty_text")
    print(f"Stats: ok={ok} | fetch_failed={failed} | empty_text={empty} | total={len(records)}")


if __name__ == "__main__":
    build_corpus()
