import json
import os
from collections import Counter, defaultdict

import spacy

IN_JSONL = "output/metiers_corpus.jsonl"
OUT_JSONL = "output/metiers_features.jsonl"

# Réglages
TOP_VERBS = 25
TOP_NOUNS = 25
MIN_TOKEN_LEN = 3


def clean_token(t: str) -> bool:
    """Filtre tokens inutiles."""
    if not t:
        return False
    if len(t) < MIN_TOKEN_LEN:
        return False
    if t.isdigit():
        return False
    return True


def main():
    if not os.path.exists(IN_JSONL):
        raise FileNotFoundError(f"Introuvable: {IN_JSONL}")

    nlp = spacy.load("fr_core_news_sm")

    with open(IN_JSONL, "r", encoding="utf-8") as f_in, open(OUT_JSONL, "w", encoding="utf-8") as f_out:
        for line in f_in:
            rec = json.loads(line)
            if rec.get("status") != "ok":
                continue

            texte = rec.get("texte", "")
            if not texte.strip():
                continue

            doc = nlp(texte)

            verb_lemmas = Counter()
            noun_lemmas = Counter()
            noun_chunks = Counter()

            for token in doc:
                if token.is_stop:
                    continue
                if token.is_punct or token.is_space:
                    continue

                lemma = token.lemma_.lower().strip()
                if not clean_token(lemma):
                    continue

                # Verbes
                if token.pos_ == "VERB":
                    verb_lemmas[lemma] += 1

                # Noms
                if token.pos_ in ("NOUN", "PROPN"):
                    noun_lemmas[lemma] += 1

            # Groupes nominaux (expressions utiles : "gestion de projet", etc.)
            for chunk in doc.noun_chunks:
                ch = chunk.text.lower().strip()
                if len(ch) >= 5:
                    noun_chunks[ch] += 1

            out = {
                "metier_onisep": rec.get("metier_onisep", ""),
                "code_rome": rec.get("code_rome", ""),
                "domaine": rec.get("domaine", ""),
                "sous_domaine": rec.get("sous_domaine", ""),
                "url_onisep": rec.get("url_onisep", ""),

                "top_verbs": [v for v, _ in verb_lemmas.most_common(TOP_VERBS)],
                "top_nouns": [n for n, _ in noun_lemmas.most_common(TOP_NOUNS)],
                "top_noun_chunks": [c for c, _ in noun_chunks.most_common(20)],

                "counts": {
                    "verbs_total": sum(verb_lemmas.values()),
                    "nouns_total": sum(noun_lemmas.values()),
                    "chunks_total": sum(noun_chunks.values()),
                }
            }

            f_out.write(json.dumps(out, ensure_ascii=False) + "\n")

    print("✅ Features métiers générées ->", OUT_JSONL)


if __name__ == "__main__":
    main()
