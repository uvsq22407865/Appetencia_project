import os
import pandas as pd

from src.excel_skills_extractor import extract_skills_from_workbook
from src.mapping_repository import load_motor_repo, map_skill_to_motor
from src.finalites_extractor import extract_finalites_from_file

DATA_CLIENTS = "data/clients"
DATA_FINALITES = "data/finalites"
REF_MOTORS = "data/referentiels/Liste des verbes moteurs _ Nicolas.xlsx"
OUT = "output/profil_sans_matching.xlsx"

os.makedirs("output", exist_ok=True)

# référentiel verbes moteurs
repo_motor = load_motor_repo(REF_MOTORS)

# ---------- 1) EXTRACTION SKILLS + VERBES MOTEURS ----------
skills_rows = []

for filename in os.listdir(DATA_CLIENTS):
    if not filename.lower().endswith(".xlsx"):
        continue

    if "referentiel" in filename.lower() or "excellence" in filename.lower():
        continue

    client_id = filename.replace(".xlsx", "")
    path = os.path.join(DATA_CLIENTS, filename)

    evidence = extract_skills_from_workbook(path, client_id)

    for e in evidence:
        verbe_moteur_long = map_skill_to_motor(e.skill, e.family, repo_motor)
        verbe_moteur_court = verbe_moteur_long.split(":")[0].strip()

        skills_rows.append({
            "client": e.client_id,
            "activite": e.activity_label,
            "sheet": e.sheet_name,
            "famille": e.family,
            "competence": e.skill,
            "dimension": e.dimension,
            "verbe_moteur_court": verbe_moteur_court,
            "verbe_moteur_long": verbe_moteur_long,
            "cellule": e.cell,
            "fichier": os.path.basename(e.source_file),
        })

df_skills = pd.DataFrame(skills_rows)
print("Skills extraites:", len(df_skills))


# ---------- 2) EXTRACTION FINALITES CANDIDAT (coche 'x') ----------
finalites_rows = []

if os.path.exists(DATA_FINALITES):
    for f in os.listdir(DATA_FINALITES):
        if not f.lower().endswith(".xlsx"):
            continue

        # client_id déduit du nom: "LS_finalites.xlsx" -> "LS"
        client_id = f.split("_")[0]
        path = os.path.join(DATA_FINALITES, f)

        selections = extract_finalites_from_file(path, client_id)
        for s in selections:
            finalites_rows.append({
                "client": s.client_id,
                "categorie": s.categorie,
                "finalite": s.finalite,
                "sheet": s.sheet,
                "fichier": os.path.basename(s.source_file),
            })

df_finalites = pd.DataFrame(finalites_rows)
print("Finalités sélectionnées:", len(df_finalites))


# ---------- 3) EXPORT EXCEL DEMO ----------
with pd.ExcelWriter(OUT, engine="openpyxl") as writer:
    df_skills.to_excel(writer, sheet_name="Skills", index=False)
    df_finalites.to_excel(writer, sheet_name="Finalites", index=False)

    # petits résumés pour la démo
    if not df_skills.empty:
        df_skills["verbe_moteur_court"].value_counts().reset_index() \
            .rename(columns={"index": "verbe_moteur", "verbe_moteur_court": "count"}) \
            .to_excel(writer, sheet_name="Top_Verbes", index=False)

    if not df_finalites.empty:
        df_finalites["finalite"].value_counts().reset_index() \
            .rename(columns={"index": "finalite", "finalite": "count"}) \
            .to_excel(writer, sheet_name="Top_Finalites", index=False)

print(" OK ->", OUT)
