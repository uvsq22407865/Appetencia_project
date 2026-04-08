import os
from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase


def infer_type_n_i_a_p(row) -> str | None:
    # Choisit le 1er "X" trouvé (selon priorité)
    if str(row.get("Ca me nourrit (N)", "")).strip().upper() == "X":
        return "N"
    if str(row.get("C'est important (I)", "")).strip().upper() == "X":
        return "I"
    if str(row.get("Ca attire mon attention (A)", "")).strip().upper() == "X":
        return "A"
    if str(row.get("Pas pour moi (P)", "")).strip().upper() == "X":
        return "P"
    return None


def ingest_one_candidate(session, candidat_id: str, path_el: Path, path_dom: Path) -> tuple[int, int]:
    # --- lire intérêts (N/I/A/P)
    df_el = pd.read_csv(path_el)
    rows_el: list[dict] = []
    for _, r in df_el.iterrows():
        section = str(r.get("Section", "")).strip()
        label = str(r.get("Element", "")).strip()
        typ = infer_type_n_i_a_p(r)
        if label and typ:
            rows_el.append({"section": section, "label": label, "type": typ})

    # --- lire domaines (I/A)
    df_dom = pd.read_csv(path_dom)
    rows_dom: list[dict] = []
    for _, r in df_dom.iterrows():
        try:
            numero = int(r.get("Numero"))
        except Exception:
            continue
        nom = str(r.get("Domaine", "")).strip()
        typ = str(r.get("Choix (I/A)", "")).strip().upper()
        if nom and typ in ("I", "A"):
            rows_dom.append({"numero": numero, "nom": nom, "type": typ})

    # --- vérifier candidat existe
    n = session.run(
        "MATCH (c:Candidat {id:$id}) RETURN count(c) AS n",
        id=candidat_id
    ).single()["n"]
    if n == 0:
        raise ValueError(f"Candidat {candidat_id} introuvable dans Neo4j (attendu: (:Candidat {{id:'{candidat_id}'}})).")

    # --- upsert intérêts
    if rows_el:
        session.run(
            """
            MATCH (c:Candidat {id:$cid})
            UNWIND $rows AS row
            MERGE (i:Interet {section: row.section, label: row.label})
            MERGE (c)-[r:A_POUR_INTERET]->(i)
            SET r.type = row.type
            """,
            cid=candidat_id,
            rows=rows_el
        )

    # --- upsert domaines
    if rows_dom:
        session.run(
            """
            MATCH (c:Candidat {id:$cid})
            UNWIND $rows AS row
            MERGE (d:Domaine {numero: row.numero})
            SET d.nom = row.nom
            MERGE (c)-[r:INTERESSE_PAR]->(d)
            SET r.type = row.type
            """,
            cid=candidat_id,
            rows=rows_dom
        )

    return len(rows_el), len(rows_dom)


def ensure_constraints(session):
    # Contrainte Domaine unique
    session.run("""
    CREATE CONSTRAINT domaine_unique IF NOT EXISTS
    FOR (d:Domaine)
    REQUIRE d.numero IS UNIQUE
    """)

    # Contrainte Interet unique (multi-propriétés)
    # Si ta version Neo4j ne supporte pas, tu peux commenter et on fera autrement.
    session.run("""
    CREATE CONSTRAINT interet_unique IF NOT EXISTS
    FOR (i:Interet)
    REQUIRE (i.section, i.label) IS UNIQUE
    """)


def batch_import(
    root_dir: str,
    uri: str,
    user: str,
    password: str,
    db_name: str,
    file_el: str = "interets.csv",
    file_dom: str = "thematiques_I_A.csv",
):
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(f"Dossier introuvable: {root.resolve()}")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    total_ok = 0
    total_skip = 0

    try:
        with driver.session(database=db_name) as session:
            ensure_constraints(session)

        for cand_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
            candidat_id = cand_dir.name  # C001, C002, ...
            path_el = cand_dir / file_el
            path_dom = cand_dir / file_dom

            if not path_el.exists() or not path_dom.exists():
                print(f"SKIP {candidat_id}: fichiers manquants ({path_el.name} / {path_dom.name})")
                total_skip += 1
                continue

            try:
                with driver.session(database=db_name) as session:
                    n_el, n_dom = ingest_one_candidate(session, candidat_id, path_el, path_dom)
                print(f"OK   {candidat_id}: {n_el} interets, {n_dom} domaines")
                total_ok += 1
            except Exception as e:
                print(f"ERR  {candidat_id}: {e}")
                total_skip += 1

    finally:
        driver.close()

    print(f"\nTerminé ✅  OK={total_ok}  SKIP/ERR={total_skip}")


if __name__ == "__main__":
    # ---- À adapter chez toi
    batch_import(
        root_dir="data_candidats",
        uri=os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "taous2001"),
        db_name=os.getenv("NEO4J_DB", "appetencia.db"),
    )
