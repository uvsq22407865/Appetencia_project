import os
import pandas as pd
import streamlit as st
from neo4j import GraphDatabase
from pyvis.network import Network
import streamlit.components.v1 as components


# -----------------------------
# Neo4j helpers
# -----------------------------
def get_driver(uri: str, user: str, password: str):
    return GraphDatabase.driver(uri, auth=(user, password))


def run_cypher(
    driver,
    query: str,
    params: dict | None = None,
    database: str = "appetencia.db",
) -> list[dict]:
    params = params or {}
    with driver.session(database=database) as session:
        res = session.run(query, params)
        return [r.data() for r in res]


# -----------------------------
# Queries
# -----------------------------
QUERY_LIST_OFFRES = """
MATCH (o:Offre)
RETURN o.id AS offreId,
       coalesce(o.titre, o.id) AS titre
ORDER BY titre
LIMIT 500
"""

QUERY_LIST_CANDIDATS = """
MATCH (c:Candidat)
RETURN c.id AS candidatId,
       coalesce(c.nom, c.id) AS nom
ORDER BY nom
LIMIT 500
"""

# ── Offre → Top candidats ───────────────────────────────────────────────────
QUERY_MATCH_OFFER_TO_CANDS = """
MATCH (o:Offre {id:$offreId})

// nbReq
MATCH (o)-[:DEMANDE]->(allReq:Competence)
WITH o, count(DISTINCT allReq) AS nbReq

// Tous les candidats
MATCH (cand:Candidat)

// Toutes les compétences du candidat (deux chemins) en minuscules
CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[:A_VECU]->(:Anecdote)-[m:MONTRE]->(c1:Competence)
  RETURN collect(DISTINCT toLower(c1.nom)) AS names1, collect(DISTINCT m.groupe) AS groupes
}
CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[:A_COMPETENCE]->(c2:Competence)
  RETURN collect(DISTINCT toLower(c2.nom)) AS names2
}

WITH o, nbReq, cand, groupes,
     names1 + [x IN names2 WHERE NOT x IN names1] AS allCandLower

// overlap = compétences de l'offre couvertes par le candidat
CALL {
  WITH o, allCandLower
  MATCH (o)-[:DEMANDE]->(rc:Competence)
  WHERE toLower(rc.nom) IN allCandLower
  RETURN count(DISTINCT rc) AS overlap
}

WITH o, nbReq, cand, groupes, overlap,
     CASE WHEN nbReq=0 THEN 0.0
          ELSE toFloat(overlap) / toFloat(nbReq)
     END AS coverage,
     CASE WHEN size(groupes)=0 THEN 0.5 ELSE
       reduce(s=0.0, g IN groupes |
         s + CASE g
               WHEN "RESULTAT" THEN 1.0
               WHEN "MAITRISE" THEN 0.7
               WHEN "PLAISIR"  THEN 0.4
               ELSE 0.5
             END
       ) / toFloat(size(groupes))
     END AS group_score

// ── domain_score ────────────────────────────────────────────────────────────
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[rc:INTERESSE_PAR]->(d:Domaine)<-[od:A_POUR_DOMAINE]-(o)
  WITH collect(DISTINCT {
    w:    CASE rc.type WHEN "I" THEN 1.0 WHEN "A" THEN 0.6 ELSE 0.0 END,
    conf: coalesce(od.confidence, 0.8)
  }) AS ds
  RETURN
    CASE WHEN size(ds)=0 THEN 0.0
         ELSE reduce(s=0.0, x IN ds | s + (x.w * x.conf)) / toFloat(size(ds))
    END AS domain_score,
    size(ds) AS domain_overlap
}

// ── interest_strength ───────────────────────────────────────────────────────
CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[ri:A_POUR_INTERET]->(:Interet)
  WITH collect(
    CASE ri.type
      WHEN "N" THEN  1.0
      WHEN "I" THEN  0.8
      WHEN "A" THEN  0.5
      WHEN "P" THEN -1.0
      ELSE 0.0
    END
  ) AS ws
  RETURN
    CASE WHEN size(ws)=0 THEN 0.5
         ELSE ((reduce(s=0.0, x IN ws | s+x) / toFloat(size(ws))) + 1.0) / 2.0
    END AS interest_strength,
    size(ws) AS nb_interets
}

// ── role_score ──────────────────────────────────────────────────────────────
// On prend le MEILLEUR rôle commun (MAX) pour éviter que les rôles subis pénalisent
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[ra:ASPIRE_A]->(r:Role)<-[:REQUIERT_PROFIL]-(o)
  WITH collect(DISTINCT {
    statut: coalesce(ra.statut, 'choisi')
  }) AS rs
  WITH rs, [x IN rs | CASE x.statut
      WHEN 'energisant'  THEN 1.0
      WHEN 'choisi'      THEN 0.8
      WHEN 'confortable' THEN 0.5
      WHEN 'subi'        THEN 0.1
      ELSE 0.5
    END] AS scores
  RETURN
    CASE WHEN size(scores)=0 THEN 0.0
         ELSE reduce(best=0.0, s IN scores | CASE WHEN s > best THEN s ELSE best END)
    END AS role_score,
    size(rs) AS role_overlap
}

// ── boost_role_skills ───────────────────────────────────────────────────────
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[:ASPIRE_A {statut:'energisant'}]->(r:Role)
                 -[:IMPLIQUE]->(rc:Competence)<-[:DEMANDE]-(o)
  RETURN count(DISTINCT rc) AS comp_dans_role
}

WITH o, cand, overlap, nbReq, coverage, group_score,
     domain_score, domain_overlap,
     interest_strength, nb_interets,
     role_score, role_overlap,
     comp_dans_role,

     (0.75*coverage + 0.25*group_score)
       * (1.0 + CASE WHEN comp_dans_role > 0 THEN 0.10 ELSE 0.0 END)
       AS skills_score,

     (  $alpha * role_score
      + $skillsW * (0.75*coverage + 0.25*group_score)
          * (1.0 + CASE WHEN comp_dans_role > 0 THEN 0.10 ELSE 0.0 END)
      + $beta * domain_score
      + $gamma * interest_strength
     ) AS score

WHERE score >= $minScore

RETURN
  o.id    AS offreId,
  coalesce(o.titre, o.id)     AS offreTitre,
  cand.id AS candidatId,
  coalesce(cand.nom, cand.id) AS candidatNom,

  overlap, nbReq,
  round(coverage*1000)/1000        AS coverage,
  round(group_score*1000)/1000     AS group_score,
  round(skills_score*1000)/1000    AS skills_score,
  comp_dans_role,

  round(domain_score*1000)/1000    AS domain_score,
  domain_overlap,

  round(role_score*1000)/1000      AS role_score,
  role_overlap,

  round(interest_strength*1000)/1000 AS interest_strength,
  nb_interets,

  round(score*1000)/1000 AS score
ORDER BY score DESC, overlap DESC
LIMIT $topK
"""

# ── Candidat → Top offres ───────────────────────────────────────────────────
QUERY_MATCH_CAND_TO_OFFERS = """
MATCH (cand:Candidat {id:$candidatId})

// ── interest_strength ───────────────────────────────────────────────────────
CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[ri:A_POUR_INTERET]->(:Interet)
  WITH collect(
    CASE ri.type
      WHEN "N" THEN  1.0
      WHEN "I" THEN  0.8
      WHEN "A" THEN  0.5
      WHEN "P" THEN -1.0
      ELSE 0.0
    END
  ) AS ws
  RETURN
    CASE WHEN size(ws)=0 THEN 0.5
         ELSE ((reduce(s=0.0, x IN ws | s+x) / toFloat(size(ws))) + 1.0) / 2.0
    END AS interest_strength,
    size(ws) AS nb_interets
}

// ── compétences du candidat (deux chemins) ──────────────────────────────────
CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[:A_VECU]->(:Anecdote)-[m:MONTRE]->(c1:Competence)
  RETURN collect(DISTINCT toLower(c1.nom)) AS names1_lower,
         collect(DISTINCT c1.nom)          AS names1_all,
         collect(DISTINCT m.groupe)        AS groupes
}

CALL {
  WITH cand
  OPTIONAL MATCH (cand)-[:A_COMPETENCE]->(c2:Competence)
  RETURN collect(DISTINCT toLower(c2.nom)) AS names2_lower,
         collect(DISTINCT c2.nom)          AS names2_all
}

WITH cand, interest_strength, nb_interets, groupes,
     names1_all + names2_all AS allCandNames,
     names1_lower + names2_lower AS allCandLower

// Toutes les offres qui partagent au moins une compétence (comparaison insensible à la casse)
MATCH (o:Offre)-[:DEMANDE]->(sharedComp:Competence)
WHERE toLower(sharedComp.nom) IN allCandLower

WITH DISTINCT cand, o, interest_strength, nb_interets, groupes, allCandLower

// nbReq
CALL {
  WITH o
  MATCH (o)-[:DEMANDE]->(r:Competence)
  RETURN count(DISTINCT r) AS nbReq
}

// overlap (insensible à la casse)
CALL {
  WITH o, allCandLower
  MATCH (o)-[:DEMANDE]->(rc:Competence)
  WHERE toLower(rc.nom) IN allCandLower
  RETURN count(DISTINCT rc) AS overlap
}

WITH cand, o, interest_strength, nb_interets, groupes, nbReq, overlap,
     CASE WHEN nbReq=0 THEN 0.0
          ELSE toFloat(overlap)/toFloat(nbReq)
     END AS coverage,
     CASE WHEN size(groupes)=0 THEN 0.5 ELSE
       reduce(s=0.0, g IN groupes |
         s + CASE g
               WHEN "RESULTAT" THEN 1.0
               WHEN "MAITRISE" THEN 0.7
               WHEN "PLAISIR"  THEN 0.4
               ELSE 0.5
             END
       ) / toFloat(size(groupes))
     END AS group_score

// ── domain_score ────────────────────────────────────────────────────────────
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[rc:INTERESSE_PAR]->(d:Domaine)<-[od:A_POUR_DOMAINE]-(o)
  WITH collect(DISTINCT {
    w:    CASE rc.type WHEN "I" THEN 1.0 WHEN "A" THEN 0.6 ELSE 0.0 END,
    conf: coalesce(od.confidence, 0.8)
  }) AS ds
  RETURN
    CASE WHEN size(ds)=0 THEN 0.0
         ELSE reduce(s=0.0, x IN ds | s + (x.w * x.conf)) / toFloat(size(ds))
    END AS domain_score,
    size(ds) AS domain_overlap
}

// ── role_score ──────────────────────────────────────────────────────────────
// On prend le MEILLEUR rôle commun (MAX) pour éviter que les rôles subis pénalisent
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[ra:ASPIRE_A]->(r:Role)<-[:REQUIERT_PROFIL]-(o)
  WITH o, collect(DISTINCT {
    statut: coalesce(ra.statut, 'choisi')
  }) AS rs
  WITH rs, [x IN rs | CASE x.statut
      WHEN 'energisant'  THEN 1.0
      WHEN 'choisi'      THEN 0.8
      WHEN 'confortable' THEN 0.5
      WHEN 'subi'        THEN 0.1
      ELSE 0.5
    END] AS scores
  RETURN
    CASE WHEN size(scores)=0 THEN 0.0
         ELSE reduce(best=0.0, s IN scores | CASE WHEN s > best THEN s ELSE best END)
    END AS role_score,
    size(rs) AS role_overlap
}

// ── boost_role_skills ───────────────────────────────────────────────────────
CALL {
  WITH cand, o
  OPTIONAL MATCH (cand)-[:ASPIRE_A {statut:'energisant'}]->(r:Role)
                 -[:IMPLIQUE]->(rc:Competence)<-[:DEMANDE]-(o)
  RETURN count(DISTINCT rc) AS comp_dans_role
}

WITH cand, o, overlap, nbReq, coverage, group_score,
     domain_score, domain_overlap,
     interest_strength, nb_interets,
     role_score, role_overlap,
     comp_dans_role,

     (0.75*coverage + 0.25*group_score)
       * (1.0 + CASE WHEN comp_dans_role > 0 THEN 0.10 ELSE 0.0 END)
       AS skills_score,

     (  $alpha * role_score
      + $skillsW * (0.75*coverage + 0.25*group_score)
          * (1.0 + CASE WHEN comp_dans_role > 0 THEN 0.10 ELSE 0.0 END)
      + $beta * domain_score
      + $gamma * interest_strength
     ) AS score

WHERE score >= $minScore

RETURN
  cand.id AS candidatId,
  coalesce(cand.nom, cand.id)  AS candidatNom,
  o.id    AS offreId,
  coalesce(o.titre, o.id)      AS offreTitre,

  overlap, nbReq,
  round(coverage*1000)/1000        AS coverage,
  round(group_score*1000)/1000     AS group_score,
  round(skills_score*1000)/1000    AS skills_score,
  comp_dans_role,

  round(domain_score*1000)/1000    AS domain_score,
  domain_overlap,

  round(role_score*1000)/1000      AS role_score,
  role_overlap,

  round(interest_strength*1000)/1000 AS interest_strength,
  nb_interets,

  round(score*1000)/1000 AS score
ORDER BY score DESC, overlap DESC
LIMIT $topK
"""

# ── Explication d'un edge candidat ↔ offre ──────────────────────────────────
QUERY_EXPLAIN_EDGE = """
// Compétences via Anecdote
OPTIONAL MATCH (cand:Candidat {id:$candidatId})
      -[:A_VECU]->(:Anecdote)-[m:MONTRE]->(c1:Competence)
WITH cand, collect(DISTINCT c1.nom) AS comps1,
          collect(DISTINCT toLower(c1.nom)) AS comps1_lower,
          collect(DISTINCT m.groupe) AS groupes1

// Compétences de l'offre qui matchent chemin 1
OPTIONAL MATCH (o:Offre {id:$offreId})-[:DEMANDE]->(req1:Competence)
WHERE toLower(req1.nom) IN comps1_lower
WITH cand, comps1_lower, groupes1,
     collect(DISTINCT req1.nom) AS matched1

// Compétences via A_COMPETENCE directe
OPTIONAL MATCH (cand)-[:A_COMPETENCE]->(c2:Competence)
WITH cand, comps1_lower, groupes1, matched1,
     collect(DISTINCT c2.nom) AS comps2,
     collect(DISTINCT toLower(c2.nom)) AS comps2_lower

// Compétences de l'offre qui matchent chemin 2 (pas déjà dans matched1)
OPTIONAL MATCH (o:Offre {id:$offreId})-[:DEMANDE]->(req2:Competence)
WHERE toLower(req2.nom) IN comps2_lower
  AND NOT toLower(req2.nom) IN comps1_lower
WITH matched1, collect(DISTINCT req2.nom) AS matched2, groupes1

RETURN
  (matched1 + matched2)[0..50] AS competencesCommunes,
  groupes1 AS groupes
"""

# ── Rôles communs candidat ↔ offre ──────────────────────────────────────────
QUERY_EXPLAIN_ROLES = """
MATCH (cand:Candidat {id:$candidatId})-[ra:ASPIRE_A]->(r:Role)<-[:REQUIERT_PROFIL]-(o:Offre {id:$offreId})
RETURN
  r.nom      AS role,
  ra.statut  AS statut,
  ra.priorite AS priorite
ORDER BY ra.priorite
"""

# ── Rôles du candidat (pour affichage sidebar) ──────────────────────────────
QUERY_CANDIDAT_ROLES = """
MATCH (c:Candidat {id:$candidatId})-[ra:ASPIRE_A]->(r:Role)
RETURN r.nom AS role, ra.statut AS statut, ra.priorite AS priorite
ORDER BY ra.priorite
LIMIT 20
"""


# -----------------------------
# Graph rendering (PyVis)
# -----------------------------
def render_graph_centered(center_label: str, rows: list[dict], mode: str) -> str:
    net = Network(height="760px", width="100%", bgcolor="#050B16", font_color="#E5E7EB")

    net.set_options(r'''
{
  "nodes": {
    "shape": "dot",
    "borderWidth": 3,
    "shadow": true,
    "font": {
      "size": 22, "face": "Inter, Arial",
      "color": "#F9FAFB", "strokeWidth": 3, "strokeColor": "#050B16"
    }
  },
  "edges": {
    "smooth": { "type": "dynamic" },
    "shadow": true,
    "color": { "color": "#22C55E", "highlight": "#A3FF12" }
  },
  "interaction": {
    "hover": true,
    "tooltipDelay": 100,
    "hideEdgesOnDrag": true,
    "navigationButtons": true,
    "keyboard": true
  },
  "physics": {
    "stabilization": { "enabled": true, "iterations": 250 },
    "barnesHut": {
      "gravitationalConstant": -28000,
      "centralGravity": 0.25,
      "springLength": 240,
      "springConstant": 0.04,
      "damping": 0.12,
      "avoidOverlap": 1
    }
  }
}
''')

    center_id = f"center:{center_label}"
    center_color = {
        "background": "#3B82F6", "border": "#93C5FD",
        "highlight": {"background": "#60A5FA", "border": "#BFDBFE"}
    }
    net.add_node(center_id, label=center_label, color=center_color, size=55)

    for r in rows:
        score      = float(r.get("score", 0.0))
        overlap    = r.get("overlap", 0)
        nb_req     = r.get("nbReq", 0)
        role_score = float(r.get("role_score", 0.0))
        boost      = r.get("comp_dans_role", 0)

        if mode == "offer":
            other_id    = f"cand:{r['candidatId']}"
            other_label = r.get("candidatNom", r["candidatId"])
        else:
            other_id    = f"off:{r['offreId']}"
            other_label = r.get("offreTitre", r["offreId"])

        if score >= 0.75:
            node_color = {"background": "#A3FF12", "border": "#D9FF8C"}
        elif score >= 0.50:
            node_color = {"background": "#FBBF24", "border": "#FDE68A"}
        else:
            node_color = {"background": "#FB7185", "border": "#FDA4AF"}

        role_badge  = " 🎭" if role_score > 0 else ""
        boost_badge = " ⚡" if boost > 0 else ""

        tooltip = (
            f"Score: {score:.3f}\n"
            f"Skills: {r.get('skills_score', 0):.3f} (overlap {overlap}/{nb_req})\n"
            f"Domaine: {r.get('domain_score', 0):.3f}\n"
            f"Rôle: {role_score:.3f} ({r.get('role_overlap', 0)} rôles communs)\n"
            f"Boost compétences-rôle: {boost}\n"
            f"Intérêts: {r.get('interest_strength', 0):.3f}"
        )

        net.add_node(
            other_id,
            label=other_label + role_badge + boost_badge,
            color=node_color,
            size=36,
            title=tooltip,
        )
        net.add_edge(
            center_id, other_id,
            title=tooltip,
            width=max(3, int(score * 10)),
        )

    return net.generate_html()


# -----------------------------
# Score breakdown helper
# -----------------------------
def score_bar(label: str, value: float, color: str, weight: str):
    pct = int(value * 100)
    st.markdown(
        f"""
        <div style="margin-bottom:6px;">
          <div style="display:flex; justify-content:space-between; font-size:12px; color:#94A3B8;">
            <span>{label} <span style="color:#64748B;">({weight})</span></span>
            <span style="color:{color}; font-weight:700;">{value:.3f}</span>
          </div>
          <div style="background:#1E293B; border-radius:4px; height:8px; margin-top:3px;">
            <div style="background:{color}; width:{pct}%; height:8px; border-radius:4px;"></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Appetencia – Smart Talent Matching", layout="wide")

st.markdown("""
<style>
.block-container {padding-top: 1.3rem; padding-bottom: 1.5rem; max-width: 1400px;}
section[data-testid="stSidebar"] {background: #0B1220;}
section[data-testid="stSidebar"] * {color: #E5E7EB !important;}
div[data-baseweb="input"] input {background: #0F172A !important; color: #E5E7EB !important;}
h1, h2, h3 {letter-spacing: -0.02em;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<h1 style="font-size:44px; font-weight:900; letter-spacing:-1px; margin-bottom:0.2rem;">
  Appetencia <span style="color:#60A5FA;">– Smart Talent Matching</span>
</h1>
<p style="margin-top:0; font-size:16px; color:#94A3B8;">
  Matching graphe : compétences · domaines · <span style="color:#A3FF12;">rôles</span> · intérêts (Neo4j)
</p>
""", unsafe_allow_html=True)

# ── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Connexion Neo4j")
    uri     = st.text_input("NEO4J_URI",      os.getenv("NEO4J_URI",      "bolt://127.0.0.1:7687"))
    user    = st.text_input("NEO4J_USER",     os.getenv("NEO4J_USER",     "neo4j"))
    pwd     = st.text_input("NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", ""), type="password")
    db_name = st.text_input("NEO4J_DB",       os.getenv("NEO4J_DB",       "appetencia.db"))

    st.header("Paramètres matching")
    mode      = st.radio("Mode", ["Offre → Candidats", "Candidat → Offres"])
    topk      = st.slider("Top K",          5,   50,  15)
    min_score = st.slider("Score minimum",  0.0, 1.0, 0.3, 0.01)

    st.header("⚖️ Coefficients du score")
    st.caption("Les 4 poids sont normalisés automatiquement pour sommer à 1.")
    raw_alpha    = st.slider("🎭 Rôles",    0, 100, 40, 5)
    raw_beta     = st.slider("🏷️ Domaines", 0, 100, 25, 5)
    raw_gamma    = st.slider("❤️ Intérêts", 0, 100, 20, 5)
    raw_skills   = st.slider("🧩 Skills",   0, 100, 15, 5)
    _total = raw_alpha + raw_beta + raw_gamma + raw_skills
    if _total == 0:
        raw_alpha, raw_beta, raw_gamma, raw_skills, _total = 40, 25, 20, 15, 100
    alpha    = raw_alpha    / _total
    beta     = raw_beta     / _total
    gamma    = raw_gamma    / _total
    skills_w = raw_skills   / _total
    st.markdown(f"""
    | Pilier | Poids normalisé |
    |---|---|
    | 🎭 Rôles | **{alpha*100:.1f}%** |
    | 🏷️ Domaines | **{beta*100:.1f}%** |
    | ❤️ Intérêts | **{gamma*100:.1f}%** |
    | 🧩 Skills | **{skills_w*100:.1f}%** |
    | **Total** | **100%** |
    """)
    st.caption("⚡ = boost +10% skills si compétences dans rôle aspiré énergisant")

if not pwd:
    st.warning("Renseigne le mot de passe Neo4j dans la sidebar.")
    st.stop()

driver = get_driver(uri, user, pwd)

col_left, col_right = st.columns([2, 1], gap="large")

# ════════════════════════════════════════════════════════════════════════════
# MODE : Offre → Candidats
# ════════════════════════════════════════════════════════════════════════════
if mode == "Offre → Candidats":
    offres = run_cypher(driver, QUERY_LIST_OFFRES, database=db_name)
    if not offres:
        st.error("Aucune offre trouvée dans Neo4j (label :Offre).")
        st.stop()

    offre_map      = {o["titre"]: o["offreId"] for o in offres}
    selected_title = col_right.selectbox("Choisir une offre", list(offre_map.keys()))
    selected_id    = offre_map[selected_title]

    rows = run_cypher(
        driver, QUERY_MATCH_OFFER_TO_CANDS,
        {"offreId": selected_id, "topK": topk, "minScore": min_score, "alpha": alpha, "beta": beta, "gamma": gamma, "skillsW": skills_w},
        database=db_name,
    )

    with col_left:
        st.subheader("Graphe  🎭 = rôle commun  ⚡ = boost compétences")
        html = render_graph_centered(selected_title, rows, mode="offer")
        components.html(html, height=690, scrolling=True)

    with col_right:
        st.subheader("Top candidats")
        if rows:
            df   = pd.DataFrame(rows)
            cols = [c for c in [
                "candidatNom", "score",
                "skills_score", "comp_dans_role",
                "domain_score", "domain_overlap",
                "role_score",   "role_overlap",
                "interest_strength",
                "overlap", "nbReq", "coverage",
            ] if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, height=240)

            st.subheader("Détail candidat")
            pick = st.selectbox(
                "Candidat",
                rows,
                format_func=lambda r: (
                    f"{r.get('candidatNom', r['candidatId'])}  "
                    f"score={r['score']}  overlap={r['overlap']}/{r.get('nbReq',0)}"
                ),
            )

            st.markdown("**Décomposition du score**")
            score_bar("🎭 Rôles",     pick.get("role_score", 0),        "#A3FF12", f"{int(alpha*100)}%")
            score_bar("🧩 Skills",    pick.get("skills_score", 0),     "#60A5FA", f"{int(skills_w*100)}%")
            score_bar("🏷️ Domaines",  pick.get("domain_score", 0),     "#34D399", f"{int(beta*100)}%")
            score_bar("❤️ Intérêts",  pick.get("interest_strength", 0),"#F472B6", f"{int(gamma*100)}%")
            st.metric("Score final", pick["score"])

            exp = run_cypher(
                driver, QUERY_EXPLAIN_EDGE,
                {"offreId": selected_id, "candidatId": pick["candidatId"]},
                database=db_name,
            )
            if exp:
                with st.expander("🧩 Compétences communes"):
                    st.write(exp[0].get("competencesCommunes", []))
                    st.write("Groupes :", exp[0].get("groupes", []))

            roles_exp = run_cypher(
                driver, QUERY_EXPLAIN_ROLES,
                {"offreId": selected_id, "candidatId": pick["candidatId"]},
                database=db_name,
            )
            if roles_exp:
                with st.expander("🎭 Rôles communs"):
                    for row in roles_exp:
                        badge = {"energisant": "🟢", "choisi": "🔵",
                                 "confortable": "🟡", "subi": "🔴"}.get(row.get("statut",""), "⚪")
                        st.write(f"{badge} **{row['role']}** — statut: {row.get('statut','?')} | priorité: {row.get('priorite','?')}")
            else:
                with st.expander("🎭 Rôles communs"):
                    st.caption("Aucun rôle commun direct (l'offre n'a pas de REQUIERT_PROFIL)")
        else:
            st.info("Aucun candidat au-dessus du seuil.")

# ════════════════════════════════════════════════════════════════════════════
# MODE : Candidat → Offres
# ════════════════════════════════════════════════════════════════════════════
else:
    cands = run_cypher(driver, QUERY_LIST_CANDIDATS, database=db_name)
    if not cands:
        st.error("Aucun candidat trouvé dans Neo4j (label :Candidat).")
        st.stop()

    cand_map      = {c["nom"]: c["candidatId"] for c in cands}
    selected_name = col_right.selectbox("Choisir un candidat", list(cand_map.keys()))
    selected_id   = cand_map[selected_name]

    cand_roles = run_cypher(
        driver, QUERY_CANDIDAT_ROLES,
        {"candidatId": selected_id},
        database=db_name,
    )
    if cand_roles:
        with st.sidebar:
            st.header("🎭 Rôles du candidat")
            for r in cand_roles:
                badge = {"energisant": "🟢", "choisi": "🔵",
                         "confortable": "🟡", "subi": "🔴"}.get(r.get("statut",""), "⚪")
                st.write(f"{badge} {r['role']} *(p.{r.get('priorite','?')})*")

    rows = run_cypher(
        driver, QUERY_MATCH_CAND_TO_OFFERS,
        {"candidatId": selected_id, "topK": topk, "minScore": min_score, "alpha": alpha, "beta": beta, "gamma": gamma, "skillsW": skills_w},
        database=db_name,
    )

    with col_left:
        st.subheader("Graphe  🎭 = rôle commun  ⚡ = boost compétences")
        html = render_graph_centered(selected_name, rows, mode="cand")
        components.html(html, height=690, scrolling=True)

    with col_right:
        st.subheader("Top offres")
        if rows:
            df   = pd.DataFrame(rows)
            cols = [c for c in [
                "offreTitre", "score",
                "skills_score", "comp_dans_role",
                "domain_score", "domain_overlap",
                "role_score",   "role_overlap",
                "interest_strength",
                "overlap", "nbReq", "coverage",
            ] if c in df.columns]
            st.dataframe(df[cols], use_container_width=True, height=240)

            st.subheader("Détail offre")
            pick = st.selectbox(
                "Offre",
                rows,
                format_func=lambda r: (
                    f"{r.get('offreTitre', r['offreId'])}  "
                    f"score={r['score']}  overlap={r['overlap']}/{r.get('nbReq',0)}"
                ),
            )

            st.markdown("**Décomposition du score**")
            score_bar("🎭 Rôles",     pick.get("role_score", 0),        "#A3FF12", f"{int(alpha*100)}%")
            score_bar("🧩 Skills",    pick.get("skills_score", 0),     "#60A5FA", f"{int(skills_w*100)}%")
            score_bar("🏷️ Domaines",  pick.get("domain_score", 0),     "#34D399", f"{int(beta*100)}%")
            score_bar("❤️ Intérêts",  pick.get("interest_strength", 0),"#F472B6", f"{int(gamma*100)}%")
            st.metric("Score final", pick["score"])

            exp = run_cypher(
                driver, QUERY_EXPLAIN_EDGE,
                {"offreId": pick["offreId"], "candidatId": selected_id},
                database=db_name,
            )
            if exp:
                with st.expander("🧩 Compétences communes"):
                    st.write(exp[0].get("competencesCommunes", []))
                    st.write("Groupes :", exp[0].get("groupes", []))

            roles_exp = run_cypher(
                driver, QUERY_EXPLAIN_ROLES,
                {"offreId": pick["offreId"], "candidatId": selected_id},
                database=db_name,
            )
            if roles_exp:
                with st.expander("🎭 Rôles communs"):
                    for row in roles_exp:
                        badge = {"energisant": "🟢", "choisi": "🔵",
                                 "confortable": "🟡", "subi": "🔴"}.get(row.get("statut",""), "⚪")
                        st.write(f"{badge} **{row['role']}** — statut: {row.get('statut','?')} | priorité: {row.get('priorite','?')}")
            else:
                with st.expander("🎭 Rôles communs"):
                    st.caption("Aucun rôle commun direct (l'offre n'a pas de REQUIERT_PROFIL)")
        else:
            st.info("Aucune offre au-dessus du seuil.")