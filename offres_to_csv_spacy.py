import re
import unicodedata
from pathlib import Path
from collections import defaultdict

import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher

from rapidfuzz import fuzz, process

# =============================
# Paths / input
# =============================
OFFRES_DIR = Path("offres_text")
OUT_DIR = Path("neo4j_csv_out")
OUT_DIR.mkdir(exist_ok=True)

MAX_DOMAINES_PER_OFFRE = 2
DOMAINE_MIN_HITS = 2


# ✅ Référentiel domaines (CSV) : doit contenir au minimum Numero + Domaine
# (si tu as aussi "Choix (I/A)" ce n'est pas gênant, on ne l'utilise pas côté offres)
DOMAINES_REF = Path("data_candidats/domaine.csv")  # <-- adapte si besoin

COMP_REF = OUT_DIR / "competences.csv"
VERBES_XLSX = Path("Liste des verbes moteurs _ Nicolas.xlsx")  # sheet 1

# =============================
# Params
# =============================
MAX_MATCHES_PER_OFFRE = 20
FUZZY_THRESHOLD = 82
TOP_CANDIDATES_PER_OFFRE = 250

# On ne bloque PLUS ces verbes, on leur met juste une pénalité
WEAK_VERBS = {
    "faire", "etre", "avoir", "aller", "mettre", "prendre",
    "donner", "recevoir", "repartir", "confier", "creer",
    "definir", "determiner"
}

SECTION_GOOD = [
    "missions", "vos missions", "responsabilites", "responsabilités",
    "description du poste", "poste", "role", "rôle",
    "profil", "profil recherche", "profil recherché",
    "competences", "compétences", "requirements", "qualifications",
    "ce que vous ferez", "ce que tu feras", "ce que vous allez faire",
    "taches", "tâches", "activites", "activités",
    "ce que nous attendons", "ce que nous recherchons"
]
SECTION_BAD = [
    "a propos", "à propos", "qui sommes", "qui sommes-nous", "nous sommes",
    "l'entreprise", "entreprise", "notre histoire", "nos valeurs",
    "rejoignez", "rejoindre", "groupe", "implantation", "chiffres cles", "chiffres clés",
    "avantages", "ce que nous offrons", "nous offrons", "benefices", "bénéfices",
    "process", "processus", "recrutement", "candidature", "pour postuler",
    "diversite", "diversité", "inclusion", "egalite", "égalité", "rgpd"
]

SKILL_CUES = [
    "competence", "compétence", "profil", "experience", "expérience",
    "maitrise", "maîtrise", "connaissance", "capable", "savoir",
    "requis", "required", "qualifications", "atouts", "atout",
    "vous etes", "vous êtes", "nous recherchons", "must have", "nice to have",
    "missions", "responsabilites", "responsabilités"
]

# =============================
# Text utils
# =============================
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def normalize_basic(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = strip_accents(s)
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokenize_keywords(label: str) -> list[str]:
    # mots-clés de base depuis l'intitulé du domaine
    norm = normalize_basic(label)
    toks = [t for t in norm.split() if len(t) >= 4 and t not in {"avec", "pour", "dans", "chez", "plus"}]
    return list(dict.fromkeys(toks))  # unique preserving order

# petit dictionnaire de boost (tu peux l’enrichir)
# -----------------------------
# DOMAIN_SYNONYMS (1..39)
# Objectif: augmenter le recall (domaine détecté même si l'intitulé exact n'apparaît pas).
# Les keywords sont en minuscules, sans accents, compatibles avec normalize_basic().
# -----------------------------
DOMAIN_SYNONYMS = {
    1: [  # Mode, Design & Esthétique
        "mode", "fashion", "textile", "couture", "stylisme", "stylist",
        "design", "designer", "esthetique", "beaute", "cosmetique",
        "bijoux", "accessoires", "luxe", "luxury", "maroquinerie",
        "tendance", "collection", "pattern", "patronnage", "merchandising"
    ],
    2: [  # Culture, Média & Communication
        "culture", "media", "journalisme", "presse", "editorial", "redaction",
        "communication", "relations presse", "rp", "evenementiel", "evenement",
        "campagne", "strategie de communication", "contenu", "content", "storytelling",
        "reseaux sociaux", "social media", "community management", "community manager",
        "audiovisuel", "podcast", "radio", "television", "tv", "cinema"
    ],
    3: [  # Technologie & Numérique
        "technologie", "numerique", "digital", "informatique", "it",
        "developpement", "dev", "developpeur", "software", "web", "mobile",
        "frontend", "backend", "fullstack", "api", "cloud", "devops",
        "cybersecurite", "securite", "reseau", "systeme", "data", "database",
        "saas", "ux", "ui", "product", "agile", "scrum"
    ],
    4: [  # Industrie & Ingénierie
        "industrie", "industriel", "ingenierie", "ingenieur", "engineering",
        "mecanique", "electrique", "electronique", "automatique",
        "qualite", "qse", "lean", "six sigma", "maintenance",
        "bureau d etudes", "cao", "dao", "catia", "solidworks",
        "process", "industrialisation", "methodes", "production industrielle"
    ],
    5: [  # Production & Artisanat
        "artisanat", "artisan", "atelier", "fabrication", "production",
        "metiers d art", "savoir faire", "menuiserie", "ebenisterie",
        "ceramique", "potterie", "tournage", "forge", "metallerie",
        "imprimerie", "broderie", "tissage", "restauration", "coutellerie"
    ],
    6: [  # Transport & Logistique
        "transport", "logistique", "supply chain", "supplychain", "chaine logistique",
        "entrepot", "stock", "inventaire", "approvisionnement", "achats",
        "planning", "ordonnancement", "expedition", "livraison",
        "douane", "import export", "import", "export", "fret", "transport routier",
        "conducteur", "chauffeur", "messagerie", "flux"
    ],
    7: [  # Construction & Immobilier
        "construction", "batiment", "bim", "immobilier", "real estate",
        "chantier", "btp", "architecture", "architecte", "maitrise d oeuvre",
        "maitrise d ouvrage", "moe", "amo", "urbanisme",
        "promotion immobiliere", "gestion locative", "syndic",
        "renovation", "travaux", "second oeuvre", "gros oeuvre"
    ],
    8: [  # Commerce & Distribution
        "commerce", "distribution", "retail", "vente", "sales", "business",
        "magasin", "boutique", "e commerce", "ecommerce", "marketplace",
        "merchandising", "category management", "negociation", "negocier",
        "relation client", "service client", "customer success", "crm",
        "prospection", "account manager", "key account", "kpi"
    ],
    9: [  # Banque & Finance
        "banque", "finance", "bancaire", "assurance", "credit",
        "investissement", "asset management", "gestion d actifs",
        "controle de gestion", "comptabilite", "audit", "risk", "risque",
        "conformite", "compliance", "kpi", "budget", "tresorerie",
        "financement", "m a", "valuation", "reporting"
    ],
    10: [  # Santé & Paramédical
        "sante", "medical", "medecin", "hopital", "clinique", "soins",
        "paramedical", "infirmier", "infirmiere", "aide soignant",
        "pharmacie", "pharmacien", "laboratoire", "bio", "biologie",
        "radiologie", "kinesitherapie", "kine", "orthophonie",
        "psychologue", "psy", "patient", "telemedecine"
    ],
    11: [  # Éducation & Formation
        "education", "enseignement", "professeur", "enseignant", "pedagogie",
        "formation", "formateur", "formatrice", "elearning", "e learning",
        "cours", "tutorat", "coaching", "apprentissage", "alternance",
        "ecole", "universite", "campus", "ingenerie pedagogique",
        "evaluation", "certification"
    ],
    12: [  # Public & Institutions
        "public", "institutions", "collectivite", "collectivites",
        "mairie", "region", "departement", "etat", "ministere",
        "service public", "administration", "fonction publique",
        "politique publique", "marche public", "marches publics",
        "subvention", "reglementaire", "juridique public"
    ],
    13: [  # Tourisme & Hôtellerie
        "tourisme", "hotel", "hotellerie", "restauration", "restaurant",
        "hospitality", "accueil", "reception", "conciergerie",
        "voyage", "travel", "agence de voyage", "reservation",
        "evenementiel", "tour operator", "booking", "service en salle",
        "cuisine", "chef"
    ],
    14: [  # Services aux Entreprises
        "services aux entreprises", "b2b", "conseil", "consulting",
        "cabinet", "prestation", "outsourcing", "externalisation",
        "juridique", "legal", "avocat", "comptabilite", "expert comptable",
        "audit", "rh", "recrutement", "marketing b2b", "business development",
        "support", "assistance", "service"
    ],
    15: [  # Sport & Loisirs
        "sport", "fitness", "coach sportif", "entrainement",
        "loisirs", "club", "association sportive",
        "evenement sportif", "competition", "esport", "e sport",
        "salle de sport", "yoga", "pilates", "outdoor", "randonee"
    ],
    16: [  # Service aux Personnes
        "service a la personne", "aide a domicile", "a domicile",
        "accompagnement", "assistance", "menage", "garde d enfants",
        "petite enfance", "auxiliaire de vie", "handicap",
        "social", "travail social", "educateur", "mediatrice", "mediatrice sociale"
    ],
    17: [  # Environnement & Nature
        "environnement", "ecologie", "biodiversite", "nature",
        "protection", "conservation", "faune", "flore",
        "parc naturel", "gestion des dechets", "dechets",
        "pollution", "qualite de l air", "eau", "assainissement",
        "rse", "csr", "impact environnemental"
    ],
    18: [  # Énergie & Transition
        "energie", "transition energetique", "renouvelable",
        "solaire", "photovoltaque", "eolien", "hydraulique",
        "biomasse", "reseau electrique", "smart grid",
        "efficacite energetique", "decarbonation", "carbone",
        "hydrogene", "batterie", "stockage"
    ],
    19: [  # Agriculture & Alimentation durable
        "agriculture", "agro", "agroalimentaire", "alimentation",
        "durable", "bio", "circuit court", "circuits courts",
        "permaculture", "maraichage", "elevage", "semence",
        "nutrition", "cantine", "restauration collective",
        "gaspillage alimentaire", "tracabilite"
    ],
    20: [  # Inclusion & Égalité
        "inclusion", "egalite", "diversite", "equite", "handicap",
        "accessibilite", "mixite", "lutte contre les discriminations",
        "non discrimination", "parite", "integration", "insertion",
        "egalite des chances", "egalite femmes hommes"
    ],
    21: [  # Habitat & Villes durables
        "habitat", "ville durable", "villes durables", "urbanisme",
        "mobilite", "mobilite douce", "amenagement", "smart city",
        "logement", "renovation energetique", "isolation",
        "eco quartier", "ecoquartier", "tiers lieu", "tiers lieux",
        "qualite de vie", "resilience urbaine"
    ],
    22: [  # Travail décent & Impact social
        "travail decent", "impact social", "impact", "social",
        "conditions de travail", "qvt", "qualite de vie au travail",
        "insertion", "emploi", "employabilite", "justice sociale",
        "responsabilite sociale", "rso", "rse", "sante au travail"
    ],
    23: [  # Humanitaire & Coopération
        "humanitaire", "cooperation", "ong", "association",
        "aide internationale", "solidarite internationale",
        "urgence", "crise", "refugies", "migration",
        "developpement", "terrain", "mission", "projet humanitaire"
    ],
    24: [  # Science & Développement durable
        "science", "recherche", "r d", "rd", "innovation",
        "developpement durable", "sustainable", "durabilite",
        "climat", "transition", "evaluation d impact", "impact",
        "publication", "laboratoire", "academique", "prototype"
    ],
    25: [  # IA & Data éthique
        "ia", "ai", "data", "donnees", "machine learning", "deep learning",
        "ethique", "ethics", "responsable", "responsible ai",
        "biais", "fairness", "transparence", "explainability",
        "rgpd", "privacy", "gouvernance des donnees", "data governance",
        "model", "modele", "mlops"
    ],
    26: [  # Économie circulaire
        "economie circulaire", "circular economy", "reemploi",
        "reutilisation", "reparation", "repair", "recyclage",
        "recycler", "upcycling", "upcycle", "seconde main",
        "anti gaspillage", "zero dechet", "zero waste",
        "ecoconception", "eco conception"
    ],
    27: [  # Bien-être & développement
        "bien etre", "bienetre", "wellbeing", "sante mentale",
        "developpement personnel", "coaching", "mindfulness",
        "meditation", "yoga", "psychologie", "resilience",
        "qualite de vie", "equilibre", "burn out", "burnout"
    ],
    28: [  # Silver economy
        "silver economy", "senior", "seniors", "personnes agees",
        "gerontologie", "ehpad", "autonomie", "dependance",
        "aidant", "aidants", "domotique", "teleassistance",
        "bien vieillir", "care"
    ],
    29: [  # Maker culture & DIY
        "maker", "diy", "do it yourself", "fablab", "atelier",
        "impression 3d", "3d printing", "laser", "cnc",
        "prototype", "prototypage", "bricolage", "open source",
        "electronique", "arduino", "raspberry pi", "iot"
    ],
    30: [  # Création digitale & nouveaux médias
        "creation digitale", "contenu digital", "nouveaux medias",
        "creator", "creators", "influence", "influenceur",
        "streaming", "twitch", "youtube", "tiktok",
        "motion design", "montage video", "video editing",
        "design graphique", "graphisme", "3d", "animation"
    ],
    31: [  # Freelancing & indépendants
        "freelance", "freelancing", "independant", "independants",
        "auto entrepreneur", "autoentrepreneur", "mission", "portage",
        "consultant", "facturation", "clients", "prospection",
        "remote", "teletravail", "contract", "contrat"
    ],
    32: [  # Robotique & automatisation
        "robotique", "robot", "automatisation", "automation",
        "automate", "plc", "siemens", "rockwell",
        "industrie 4 0", "industry 4 0", "cobot", "cobotics",
        "vision", "computer vision", "capteur", "capteurs",
        "iot", "iiot", "maintenance predictive"
    ],
    33: [  # Slow life & modes de vie alternatifs
        "slow life", "mode de vie", "alternatif", "alternatifs",
        "sobriete", "minimalisme", "minimalist",
        "low tech", "lowtech", "deconnexion", "deconnecter",
        "simplicite", "transition", "eco village", "ecovillage",
        "habitat participatif"
    ],
    34: [  # Médiation & intelligence collective
        "mediation", "facilitation", "intelligence collective",
        "co design", "codesign", "atelier participatif",
        "concertation", "gouvernance", "sociocratie",
        "animation", "collectif", "cooperation", "resolution de conflit",
        "negociation", "dialogue"
    ],
    35: [  # Économie sociale & solidaire
        "economie sociale", "economie solidaire", "ess",
        "association", "cooperative", "scop", "scic",
        "fondation", "entreprise sociale", "impact",
        "insertion", "solidarite", "utilite sociale", "tiers secteur"
    ],
    36: [  # Reconnexion au vivant
        "reconnexion", "vivant", "nature", "biodiversite",
        "ecopsychologie", "ecotherapie", "plein air",
        "foret", "jardin", "horticulture", "permaculture",
        "observation", "ecosysteme", "sensibilisation"
    ],
    37: [  # Gamification & design d’expérience
        "gamification", "game design", "gameplay", "mecanique de jeu",
        "ux", "user experience", "design d experience",
        "experience utilisateur", "parcours utilisateur", "engagement",
        "serious game", "serious games", "ludification",
        "reward", "points", "badges", "leaderboard"
    ],
    38: [  # Sciences humaines
        "sciences humaines", "sociologie", "psychologie",
        "anthropologie", "philosophie", "histoire",
        "science politique", "politologie", "ethnologie",
        "recherche qualitative", "qualitative", "etude", "enquete",
        "analyse", "terrain"
    ],
    39: [  # Arts visuels / vivants / narratifs / sonores
        "arts visuels", "arts vivants", "art", "spectacle vivant",
        "theatre", "danse", "musique", "son", "sound",
        "creation", "scenographie", "mise en scene",
        "photographie", "photo", "illustration", "dessin",
        "narration", "ecriture", "edition", "auteur"
    ],
}



def extract_title(raw: str, fallback: str) -> str:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    return lines[0][:120] if lines else fallback

def looks_like_heading(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if len(s) > 90:
        return False
    if s.endswith(":"):
        return True
    letters = re.sub(r"[^A-Za-zÀ-ÿ]+", "", s)
    if letters and letters.upper() == letters and len(letters) >= 6:
        return True
    if s.startswith(("-", "•", "*")):
        return False
    return False

def extract_sections_soft(raw: str) -> str:
    """
    Filtrage SOFT :
    - si on détecte des sections GOOD, on privilégie ces lignes
    - mais on garde quand même un peu du reste (au lieu de jeter tout)
    """
    lines = raw.splitlines()

    kept_good = []
    kept_other = []

    active = False
    in_bad = False
    found_good = False

    for ln in lines:
        t = normalize_basic(ln)

        if looks_like_heading(ln):
            in_bad = any(k in t for k in SECTION_BAD)
            active = any(k in t for k in SECTION_GOOD)
            if active:
                found_good = True
            continue

        if in_bad:
            continue

        if active:
            kept_good.append(ln)
        else:
            kept_other.append(ln)

    if not found_good:
        return raw

    other_text = "\n".join([l for l in kept_other if l.strip()])
    good_text = "\n".join([l for l in kept_good if l.strip()])

    other_slice = other_text[:1500]
    return (good_text + "\n\n" + other_slice).strip()

# =============================
# spaCy
# =============================
nlp = spacy.load("fr_core_news_sm", disable=["ner"])

def load_verbes_moteurs(xlsx_path: Path) -> set[str]:
    if not xlsx_path.exists():
        return set()
    df = pd.read_excel(xlsx_path, sheet_name=0, header=None)
    verbs = set()
    for col in df.columns:
        for v in df[col].dropna().astype(str):
            w = normalize_basic(v)
            if w and len(w) >= 3 and len(w.split()) <= 3:
                verbs.add(w)
    return verbs

verbs_moteurs = load_verbes_moteurs(VERBES_XLSX)
print("Verbes moteurs chargés:", len(verbs_moteurs))

def sentence_weight(sent: str) -> float:
    s_norm = normalize_basic(sent)
    w = 1.0
    if sent.lstrip().startswith(("-", "•", "*")):
        w *= 1.5
    if any(cue in s_norm for cue in SKILL_CUES):
        w *= 1.2
    if "responsable" in s_norm or "en charge" in s_norm:
        w *= 1.1
    return w

def extract_verbs_and_nps(text: str):
    """
    Extrait:
      - verbes (lemmes)
      - groupes nominaux (noun chunks) normalisés
    Retourne liste de tuples (candidate_text, kind, evidence_sentence, score_weight)
    """
    doc = nlp(text)
    candidates = []

    # 1) par phrase: verbes + scoring
    for sent in doc.sents:
        s = sent.text.strip()
        if not s:
            continue
        w_sent = sentence_weight(s)

        for t in sent:
            if t.is_space or t.is_punct or t.is_stop:
                continue
            if t.pos_ not in ("VERB", "AUX"):
                continue
            lem = normalize_basic(t.lemma_)
            if len(lem) < 3:
                continue

            w = w_sent
            if lem in verbs_moteurs:
                w *= 1.25
            if lem in WEAK_VERBS:
                w *= 0.35

            candidates.append((lem, "VERB", s[:220], w))

    # 2) groupes nominaux
    try:
        for chunk in doc.noun_chunks:
            chunk_txt = chunk.text.strip()
            norm = normalize_basic(chunk_txt)
            if len(norm) < 4:
                continue
            if norm in {"entreprise", "groupe", "projet", "equipe", "client"}:
                continue
            candidates.append((norm, "NP", chunk_txt[:220], 0.8))
    except Exception:
        pass

    return candidates

# =============================
# Load skills reference
# =============================
if not COMP_REF.exists():
    raise FileNotFoundError(f"{COMP_REF} introuvable. Lance d'abord exceltocsv_multi.py")

df_comp = pd.read_csv(COMP_REF)

ref_norm_list = []
ref_norm_to_id = {}
ref_id_to_name = {}

for _, row in df_comp.iterrows():
    cid = str(row["competenceId"])
    cname = str(row["nom"])
    ref_id_to_name[cid] = cname

    norm_name = normalize_basic(cname)

    doc = nlp(cname)
    lemmas = []
    for t in doc:
        if t.is_space or t.is_punct or t.is_stop:
            continue
        lemmas.append(normalize_basic(t.lemma_))
    lem_name = " ".join([x for x in lemmas if x])

    keys = set([norm_name, lem_name])
    keys = {k for k in keys if k and len(k) >= 3}

    for k in keys:
        ref_norm_list.append(k)
        ref_norm_to_id.setdefault(k, cid)

# =============================
# Matching candidates -> ref
# =============================
def best_ref_match(candidate: str):
    if candidate in ref_norm_to_id:
        cid = ref_norm_to_id[candidate]
        return cid, candidate, 100

    best = process.extractOne(candidate, ref_norm_list, scorer=fuzz.ratio)
    if not best:
        return None, None, 0
    match_key, score, _ = best
    if score < FUZZY_THRESHOLD:
        return None, None, score
    cid = ref_norm_to_id.get(match_key)
    return cid, match_key, score

# =============================
# Domaines ref + matcher
# =============================
def load_domaines_ref(path: Path) -> list[tuple[int, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} introuvable. "
            f"Attendu: un CSV avec colonnes au moins ['Numero','Domaine']."
        )
    df = pd.read_csv(path)
    # tolérance de noms de colonnes
    cols = {c.lower(): c for c in df.columns}
    col_num = cols.get("numero") or cols.get("num")
    col_dom = cols.get("domaine") or cols.get("nom") or cols.get("intitule") or cols.get("intitulé") or cols.get("title")

    if not col_num or not col_dom:
        raise ValueError(
            f"{path} ne contient pas les colonnes attendues. Colonnes trouvées: {list(df.columns)} "
            f"(attendu au moins Numero + Domaine)"
        )

    out = []
    for _, r in df.iterrows():
        try:
            numero = int(r[col_num])
        except Exception:
            continue
        nom = str(r[col_dom]).strip()
        if nom:
            out.append((numero, nom))
    return out

domaines_ref = load_domaines_ref(DOMAINES_REF)
domain_keywords = []
for numero, nom in domaines_ref:
    kws = tokenize_keywords(nom)
    kws += DOMAIN_SYNONYMS.get(numero, [])
    kws = [normalize_basic(k) for k in kws if k]
    kws = [k for k in kws if len(k) >= 3]
    kws = list(dict.fromkeys(kws))
    domain_keywords.append((numero, nom, kws))

print("Domaines ref chargés:", len(domaines_ref))




domain_matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
domain_patterns = [nlp.make_doc(nom) for _, nom in domaines_ref]
domain_matcher.add("DOMAINE", domain_patterns)

# map lower(name)->(numero, name) for exact lookup
domain_by_lower = {nom.strip().lower(): (numero, nom) for numero, nom in domaines_ref}

# =============================
# Read offers
# =============================
files = sorted(OFFRES_DIR.glob("OFF_*.txt"))
if not files:
    raise FileNotFoundError("Aucune offre trouvée. Mets des fichiers OFF_001.txt dans offres_text/")

offres_rows = []
offre_comp_rows = []
offre_dom_rows = []

for f in files:
    offre_id = f.stem
    raw = f.read_text(encoding="utf-8", errors="ignore")
    titre = extract_title(raw, f"Offre {offre_id}")

    filtered = extract_sections_soft(raw)

    offres_rows.append({
        "offreId": offre_id,
        "titre": titre,
        "source": "txt_export",
        "url": "",
        "texte": raw,
        "texteFiltre": filtered
    })

        # ---------- DOMAINE EXTRACTION (OFFRE -> DOMAINE) ----------
    text_norm = normalize_basic(filtered)

    scored = []
    for numero, nom, kws in domain_keywords:
        hits = 0
        for kw in kws:
            if kw and kw in text_norm:
                hits += 1
        if hits > 0:
            scored.append((hits, numero, nom))

    scored.sort(reverse=True)  # hits desc

    selected = scored[:MAX_DOMAINES_PER_OFFRE]
    for hits, numero, nom in selected:
        if hits < DOMAINE_MIN_HITS:
            continue
        # confidence simple: plus y a de hits, plus c’est haut
        conf = min(0.95, 0.35 + 0.15 * hits)

        offre_dom_rows.append({
            "offreId": offre_id,
            "numero": numero,
            "domaine": nom,
            "confiance": round(conf, 3),
            "sourceMethod": "keyword_scoring",
            "evidence": f"hits={hits}"
        })

    # ---------- SKILL EXTRACTION (OFFRE -> COMPETENCE) ----------
    candidates = extract_verbs_and_nps(filtered)

    agg_score = defaultdict(float)
    best_evidence = {}
    best_kind = {}

    for cand_txt, kind, evid, w in candidates:
        agg_score[cand_txt] += w
        if cand_txt not in best_evidence:
            best_evidence[cand_txt] = evid
            best_kind[cand_txt] = kind

    cand_sorted = sorted(agg_score.items(), key=lambda x: -x[1])[:TOP_CANDIDATES_PER_OFFRE]

    seen_comp = set()
    rows_for_offer = []

    for cand_txt, score_cand in cand_sorted:
        cid, match_key, match_score = best_ref_match(cand_txt)
        if not cid:
            continue

        conf = (match_score / 100.0)

        if score_cand < 1.0:
            conf *= 0.95

        if cand_txt in WEAK_VERBS:
            conf *= 0.7

        conf = round(min(1.0, conf), 3)

        if cid in seen_comp:
            continue
        seen_comp.add(cid)

        rows_for_offer.append({
            "offreId": offre_id,
            "competenceId": cid,
            "confiance": conf,
            "sourceMethod": "hybrid_soft_sections+verbs+NP+spacy+fuzzy",
            "candidateText": cand_txt,
            "candidateKind": best_kind.get(cand_txt, ""),
            "evidence": best_evidence.get(cand_txt, ""),
            "candidateScore": round(score_cand, 3),
            "matchKey": match_key,
            "matchScore": match_score
        })

        if len(rows_for_offer) >= MAX_MATCHES_PER_OFFRE:
            break

    offre_comp_rows.extend(rows_for_offer)

print("OK ✅ Extraction terminée")
print("Nb offres:", len(offres_rows))
print("Nb liens DEMANDE:", len(offre_comp_rows))
print("Nb liens DOMAINE:", len(offre_dom_rows))

pd.DataFrame(offres_rows).to_csv(OUT_DIR / "offres.csv", index=False)
pd.DataFrame(offre_comp_rows).to_csv(OUT_DIR / "offre_competence.csv", index=False)
pd.DataFrame(offre_dom_rows).to_csv(OUT_DIR / "offre_domaine.csv", index=False)

print("CSV généré :", (OUT_DIR / "offres.csv").resolve())
print("CSV généré :", (OUT_DIR / "offre_competence.csv").resolve())
print("CSV généré :", (OUT_DIR / "offre_domaine.csv").resolve())
