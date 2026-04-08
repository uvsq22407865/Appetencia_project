"""
extract_roles_offres.py
=======================
Extrait les rôles Appetencia depuis les offres d'emploi et génère :
  - neo4j_csv_out/offre_role.csv  →  LOAD CSV pour créer (:Offre)-[:REQUIERT_PROFIL]->(:Role)

Stratégie (du plus fiable au moins fiable) :
  1. Titre (1ère ligne) — mapping direct + synonymes
  2. Phrase d'intro (2-3 premières phrases) — pattern "en tant que X", "poste de X"
  3. Fallback : scan du texte filtré
"""

import re
import unicodedata
from pathlib import Path
from collections import defaultdict

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
OFFRES_DIR = Path("offres_text")
OUT_DIR    = Path("neo4j_csv_out")
OUT_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Utils texte
# ─────────────────────────────────────────────────────────────────────────────
def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))

def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = strip_accents(s)
    s = re.sub(r"[^a-z0-9\s/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ─────────────────────────────────────────────────────────────────────────────
# Les 58 rôles Appetencia + leurs synonymes métier
# ─────────────────────────────────────────────────────────────────────────────
# Structure : { role_nom : [synonymes normalisés] }
# Les synonymes sont les intitulés de poste typiques qui mappent vers ce rôle.
ROLE_SYNONYMS: dict[str, list[str]] = {

    # ── Accompagnement ──────────────────────────────────────────────────────
    "Accompagnateur": [
        "accompagnateur", "accompagnatrice", "chargé d accompagnement",
        "conseiller en insertion", "chargé de suivi", "case manager",
        "référent parcours", "coordinateur parcours",
    ],
    "Conseiller": [
        "conseiller", "conseillère", "consultant", "consultante",
        "chargé de conseil", "advisor", "consultant senior",
        "consultant junior", "business advisor",
    ],
    "Pédagogue": [
        "pedagogue", "formateur", "formatrice", "enseignant", "enseignante",
        "professeur", "professeure", "instructeur", "instructrice",
        "ingenieur pedagogique", "concepteur pedagogique",
        "learning designer", "trainer",
    ],
    "Mentor": [
        "mentor", "mentore", "coach", "coache", "life coach",
        "coach de vie", "coach professionnel", "accompagnateur de carriere",
    ],
    "Facilitateur": [
        "facilitateur", "facilitatrice", "facilitation", "animateur de groupe",
        "coach collectif", "moderateur", "mediateur de processus",
        "facilitateur agile", "scrum master",
    ],
    "Guide": [
        "guide", "guilde", "guide touristique", "guide nature",
        "guide conferencier", "directeur de parcours",
    ],

    # ── Création ────────────────────────────────────────────────────────────
    "Artiste": [
        "artiste", "artiste plasticien", "peintre", "sculpteur",
        "illustrateur", "illustratrice", "dessinateur", "dessinatrice",
    ],
    "Créateur": [
        "createur", "creatrice", "creator", "creative director",
        "directeur artistique", "directrice artistique",
        "createur de contenu", "content creator",
    ],
    "Amuseur": [
        "animateur", "animatrice", "entertaineur", "clown", "comique",
        "stand up", "humoriste",
    ],
    "Acteur": [
        "acteur", "actrice", "comedien", "comedienne", "performeur",
    ],
    "Réparateur": [
        "reparateur", "reparatrice", "technicien de maintenance",
        "technicienne de maintenance", "agent de maintenance",
        "technicien sav", "responsable sav",
    ],
    "Bricoleur": [
        "bricoleur", "bricoleuse", "technicien polyvalent",
        "technicien batiment", "agent technique",
    ],
    "Inventeur": [
        "inventeur", "inventrice",
        "ingenieur r d", "ingenieure r d",
    ],
    "Innovateur": [
        "innovateur", "innovatrice", "responsable innovation",
        "directeur innovation", "chief innovation officer", "cio",
        "intraprenneur", "intrapreneuse",
    ],

    # ── Leadership ───────────────────────────────────────────────────────────
    "Manager": [
        "manager", "manageur", "manageuse", "responsable d equipe",
        "team lead", "team leader", "chef d equipe", "superviseur",
        "superviseuse", "team manager", "people manager",
    ],
    "Leader": [
        "leader", "leadeuse",
        "vice president", "vp", "head of",
        "directeur general", "directrice generale",
        "responsable strategique", "responsable de division",
        "responsable de departement", "head of department",
    ],
    "Dirigeant": [
        "dirigeant", "dirigeante", "dg", "directeur general",
        "directrice generale", "ceo", "chief executive",
        "president", "presidente", "coo", "directeur des operations",
    ],
    "Chef de projet": [
        "chef de projet", "chef de projets", "project manager",
        "pm", "project lead",
        "responsable projet", "responsable de projet", "responsable projets",
        "responsable de projets", "responsable programme",
        "charge de projet", "chargee de projet",
        "coordinateur de projet", "coordinatrice de projet",
        "gestionnaire de projet", "pilote de projet",
        "directeur de projet", "directrice de projet",
        "program manager", "portfolio manager",
    ],
    "Initiateur": [
        "initiateur", "initiatrice", "fondateur", "fondatrice",
        "co fondateur", "co fondatrice", "launcher", "porteur de projet",
        "entrepreneur", "entrepreneuse",
    ],
    "Visionnaire": [
        "visionnaire", "chief vision officer", "cvo", "stratege",
        "directeur de la strategie", "chief strategy officer",
    ],
    "Stratège": [
        "stratege", "strategiste", "responsable strategie",
        "strategy manager", "business strategist", "analyste strategique",
        "directeur strategie", "chief strategy officer", "cso",
    ],
    "Décideur": [
        "decideur", "decision maker", "daf", "directeur administratif",
        "directeur financier", "chief financial officer", "cfo",
    ],
    "Porteur de projet": [
        "porteur de projet", "porteuse de projet", "chef de projet porteur",
        "responsable programme", "program manager",
    ],

    # ── Relation ─────────────────────────────────────────────────────────────
    "Médiateur": [
        "mediateur", "mediatrice", "conciliateur", "conciliatrice",
        "mediateur professionnel", "agent de mediation",
        "charge de mediation",
    ],
    "Conciliateur": [
        "conciliateur", "conciliatrice", "arbitre", "negociateur social",
    ],
    "Connecteur": [
        "connecteur", "connectrice", "chargé de partenariats",
        "responsable partenariats", "partnership manager",
        "business developer", "developpeur commercial",
    ],
    "Réseauteur": [
        "reseauteur", "reseauteuse", "community builder",
        "network manager", "responsable relations exterieures",
        "chargé de relations", "charge des relations",
    ],
    "Traducteur": [
        "traducteur", "traductrice", "interprete", "interpretatrice",
        "redacteur technique", "technical writer",
    ],
    "Animateur": [
        "animateur", "animatrice", "animateur socio culturel",
        "animateur pedagogique", "chef animateur", "animateur jeunesse",
        "animateur communautaire", "community manager",
    ],
    "Soutien / Écoute": [
        "soutien", "ecoutant", "ecoutante", "aidant", "aidante",
        "assistant social", "assistante sociale", "travailleur social",
        "travailleuse sociale", "auxiliaire de vie", "aide a domicile",
    ],
    "Négociateur": [
        "negociateur", "negociatrice", "acheteur negociateur",
        "responsable achats", "purchasing manager", "buyer",
    ],

    # ── Analyse ──────────────────────────────────────────────────────────────
    "Observateur": [
        "observateur", "observatrice", "auditeur", "auditrice",
        "inspecteur", "inspectrice", "controleur", "controleuse",
    ],
    "Chercheur": [
        "chercheur", "chercheuse", "researcher", "research analyst",
        "ingenieur de recherche", "ingenieure de recherche",
        "charge de recherche", "chargee de recherche",
        "research engineer", "ingenieur recherche",
        "charge d etudes", "chargee d etudes",
        "analyste recherche", "veilleur", "charge de veille",
    ],
    "Aventurier": [
        "aventurier", "aventuriere", "explorateur", "exploratrice",
        "growth hacker", "pionnier", "pionniere",
    ],
    "Testeur": [
        "testeur", "testeuse", "qa", "quality assurance",
        "ingenieur qa", "charge de tests", "test manager",
        "test engineer", "uat",
    ],
    "Analyste": [
        "analyste", "analyst", "data analyst", "business analyst",
        "ingenieur analyse", "analyste metier", "analyste fonctionnel",
        "analyste financier", "financial analyst",
    ],
    "Enquêteur": [
        "enqueteur", "enquetrice", "investigateur", "investigatrice",
        "charge d etudes", "chargée d etudes",
    ],

    # ── Expertise ────────────────────────────────────────────────────────────
    "Expert": [
        "expert", "experte", "specialiste", "spécialiste",
        "referent technique", "referente technique",
        "expert metier", "senior consultant",
    ],
    "Support technique": [
        "support technique", "technicien support", "technicienne support",
        "helpdesk", "help desk", "hotliner", "charge de support",
        "customer support", "support engineer", "it support",
    ],
    "Sachant": [
        "sachant",
        "knowledge manager", "responsable knowledge",
        "responsable documentation", "charge de documentation",
        "documentaliste", "gestionnaire de connaissances",
        "referent technique", "referente technique",
        "expert technique", "expert metier",
        "responsable qualite documentaire",
    ],
    "Contrôleur": [
        "controleur", "contrôleur", "controleuse", "auditeur interne",
        "responsable controle", "quality controller", "compliance officer",
        "charge de conformite",
    ],
    "Budgeteur": [
        "budgeteur", "budgeteuse", "controleur de gestion",
        "contrôleur de gestion", "financial controller",
        "responsable budget", "charge de budget", "comptable",
    ],
    "Juriste": [
        "juriste", "avocat", "avocate", "responsable juridique",
        "directeur juridique", "counsel", "legal counsel",
        "paralegal", "charge d affaires juridiques",
    ],
    "Statisticien": [
        "statisticien", "statisticienne", "data scientist",
        "biostatisticien", "econometriste", "modelisateur",
    ],

    # ── Communication ────────────────────────────────────────────────────────
    "Vendeur": [
        "vendeur", "vendeuse", "commercial", "commerciale",
        "charge d affaires", "ingenieur commercial", "sales",
        "account executive", "key account manager", "kam",
        "business developer", "responsable commercial",
    ],
    "Communicant": [
        "communicant", "communicante",
        "charge de communication", "chargee de communication",
        "responsable communication", "responsable de la communication",
        "communication manager", "directeur de la communication",
        "community manager", "content manager", "content strategist",
        "redacteur", "redactrice", "copywriter",
        "charge de contenu", "responsable editorial",
        "attache de presse", "relations publiques",
    ],
    "Acheteur": [
        "acheteur", "acheteuse", "buyer", "purchasing manager",
        "responsable achats", "category manager", "approvisionneur",
    ],
    "Orateur": [
        "orateur", "oratrice", "speaker", "conferencier", "conferenciere",
        "presentateur", "presentatrice", "animateur tv", "animateur radio",
    ],

    # ── Soin ─────────────────────────────────────────────────────────────────
    "Soignant": [
        "soignant", "soignante", "infirmier", "infirmiere", "aide soignant",
        "aide soignante", "medecin", "kinesitherapeute", "pharmacien",
        "pharmacienne", "sage femme", "orthophoniste", "psychologue",
    ],

    # ── Exécution ────────────────────────────────────────────────────────────
    "Exécutant": [
        "executant", "executante", "operateur", "operatrice",
        "agent de production", "agent d execution",
        "technicien de ligne", "agent logistique",
    ],

    # ── Autres ──────────────────────────────────────────────────────────────
    "Confident": [
        "confident", "confidente", "psychologue du travail",
        "responsable bien etre", "wellbeing manager",
    ],
    "Chercheur de solutions": [
        "chercheur de solutions", "problem solver",
        "ingenieur solutions", "solution architect",
        "architecte de solutions",
    ],
    "Le critique": [
        "critique", "evaluateur", "evaluatrice", "reviewer",
        "redacteur critique", "editorialiste",
    ],
    "Le bon élève": [
        "assistant", "assistante", "charge de mission",
        "chargée de mission", "junior", "stagiaire",
        "apprenti", "alternant",
    ],
    "Le cancre": [],   # rarement dans les offres
    "Le rebelle": [
        "intraprenneur", "intrapreneuse", "change maker",
        "responsable transformation", "chief transformation officer",
    ],
    "Le juge": [
        "juge", "arbitre", "commissaire", "president de jury",
    ],
    "Le sauveur": [
        "responsable crise", "gestionnaire de crise",
        "risk manager", "responsable continuité",
        "responsable securite", "hsse",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Patterns de détection dans le texte
# "en tant que X", "poste de X", "nous recherchons un X", etc.
# ─────────────────────────────────────────────────────────────────────────────
INTRO_PATTERNS = [
    r"en tant que\s+([^,\n\.]{3,60})",
    r"poste\s+(?:de|d[' ])\s+([^,\n\.]{3,60})",
    r"(?:nous recherchons|nous recrutons|offre pour)\s+(?:un|une|un·e|un\.e)?\s+([^,\n\.]{3,60})",
    r"(?:rejoignez[- ]nous en tant que|rejoindre notre equipe comme)\s+([^,\n\.]{3,60})",
    r"(?:recrutement|cdi|cdd|stage|alternance)\s*[:\-]?\s*([^,\n\.]{3,60})",
    r"^([^,\n\.]{3,80})$",   # titre seul sur une ligne (1ère ligne)
]

# ─────────────────────────────────────────────────────────────────────────────
# Index inversé : synonyme normalisé → role_nom
# ─────────────────────────────────────────────────────────────────────────────
def build_synonym_index(role_synonyms: dict) -> dict[str, str]:
    idx = {}
    for role_nom, synonymes in role_synonyms.items():
        # le nom du rôle lui-même
        idx[norm(role_nom)] = role_nom
        for syn in synonymes:
            n = norm(syn)
            if n and n not in idx:
                idx[n] = role_nom
    return idx

SYNONYM_IDX = build_synonym_index(ROLE_SYNONYMS)

# tri par longueur décroissante pour faire du longest-match d'abord
SYNONYM_KEYS_SORTED = sorted(SYNONYM_IDX.keys(), key=len, reverse=True)


def find_roles_in_text(text: str, max_roles: int = 3) -> list[dict]:
    """
    Cherche les rôles Appetencia dans un texte normalisé.
    Retourne une liste de dicts {role, confidence, method, evidence}.
    """
    text_n = norm(text)
    found: dict[str, dict] = {}  # role_nom -> meilleur hit

    def add_hit(role_nom: str, conf: float, method: str, evid: str):
        if role_nom not in found or found[role_nom]["confidence"] < conf:
            found[role_nom] = {
                "role":       role_nom,
                "confidence": round(conf, 3),
                "method":     method,
                "evidence":   evid[:200],
            }

    # ── 1. Scan par synonymes (longest-match) ──────────────────────────────
    for key in SYNONYM_KEYS_SORTED:
        if key in text_n:
            role_nom = SYNONYM_IDX[key]
            # boost si dans le titre (premiers 120 chars)
            in_title = key in text_n[:120]
            conf = 0.95 if in_title else 0.80
            add_hit(role_nom, conf, "synonym_title" if in_title else "synonym_body", key)

    # ── 2. Patterns "en tant que…", "poste de…" ───────────────────────────
    # On prend les 3 premières lignes = intro
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    intro = "\n".join(lines[:5])
    intro_n = norm(intro)

    for pat in INTRO_PATTERNS:
        for m in re.finditer(pat, intro_n):
            fragment = m.group(1).strip()
            # chercher un synonyme dans ce fragment
            for key in SYNONYM_KEYS_SORTED:
                if key in fragment:
                    role_nom = SYNONYM_IDX[key]
                    add_hit(role_nom, 0.92, "intro_pattern", fragment)
                    break  # 1 rôle par match de pattern

    # ── Tri par confiance ──────────────────────────────────────────────────
    results = sorted(found.values(), key=lambda x: -x["confidence"])
    return results[:max_roles]


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────
def extract_title(raw: str, fallback: str) -> str:
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    return lines[0][:120] if lines else fallback


files = sorted(OFFRES_DIR.glob("OFF_*.txt"))
if not files:
    raise FileNotFoundError("Aucune offre dans offres_text/. Crée des fichiers OFF_001.txt")

offre_role_rows = []
summary = []

for f in files:
    offre_id = f.stem
    raw      = f.read_text(encoding="utf-8", errors="ignore")
    titre    = extract_title(raw, f"Offre {offre_id}")

    # On cherche dans titre + intro (500 premiers chars)
    search_zone = raw[:500]
    roles_found = find_roles_in_text(search_zone, max_roles=3)

    if not roles_found:
        # fallback : scan du texte complet mais conf réduite
        roles_found = find_roles_in_text(raw, max_roles=2)
        for r in roles_found:
            r["confidence"] = round(r["confidence"] * 0.7, 3)
            r["method"]    += "_fallback"

    for r in roles_found:
        offre_role_rows.append({
            "offreId":    offre_id,
            "titre":      titre,
            "role":       r["role"],
            "confidence": r["confidence"],
            "method":     r["method"],
            "evidence":   r["evidence"],
        })

    summary.append({
        "offre":  offre_id,
        "titre":  titre[:60],
        "roles":  [r["role"] for r in roles_found],
        "conf":   [r["confidence"] for r in roles_found],
    })

# ─────────────────────────────────────────────────────────────────────────────
# Export CSV
# ─────────────────────────────────────────────────────────────────────────────
df_out = pd.DataFrame(offre_role_rows)
out_path = OUT_DIR / "offre_role.csv"
df_out.to_csv(out_path, index=False, encoding="utf-8")

print(f"\n✅ {len(offre_role_rows)} liens offre→rôle générés")
print(f"📄 Fichier : {out_path.resolve()}\n")

print("── Résumé par offre ──────────────────────────────────────")
for s in summary:
    roles_str = ", ".join(
        f"{r} ({c})" for r, c in zip(s["roles"], s["conf"])
    ) or "⚠️  AUCUN RÔLE DÉTECTÉ"
    print(f"  {s['offre']} | {s['titre']}")
    print(f"         → {roles_str}")
print("──────────────────────────────────────────────────────────")

print("""
══════════════════════════════════════════════════════════
IMPORT NEO4J — colle dans Neo4j Browser
══════════════════════════════════════════════════════════

// 1. Copie offre_role.csv dans ton dossier import/

// 2. Lance cette requête :

LOAD CSV WITH HEADERS FROM 'file:///offre_role.csv' AS row
MATCH (o:Offre {id: row.offreId})
MATCH (r:Role  {nom: row.role})
MERGE (o)-[rel:REQUIERT_PROFIL]->(r)
ON CREATE SET
  rel.confidence = toFloat(row.confidence),
  rel.method     = row.method,
  rel.evidence   = row.evidence
ON MATCH SET
  rel.confidence = toFloat(row.confidence);

// 3. Vérification :
// MATCH (o:Offre)-[r:REQUIERT_PROFIL]->(role:Role)
// RETURN o.titre, role.nom, r.confidence
// ORDER BY r.confidence DESC LIMIT 20;
══════════════════════════════════════════════════════════
""")