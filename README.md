# Appetencia — Système de recommandation basé sur un graphe

> Projet Master 2 DataScale — UVSQ / Université Paris-Saclay  
> Étudiante : Taous Meziane  
> Encadrante : Zoubida KEDAD

---

## Description

Ce projet implémente un système de recommandation professionnelle basé sur une base de données graphe Neo4j. Il met en relation des profils candidats issus du **Bilan d'Appétences** (Appetencia) avec des offres d'emploi réelles, en s'appuyant sur un score multi-dimensionnel combinant compétences, rôles aspirés, domaines et intérêts.

---

## Structure du projet

```
Appetencia_project/
├── app_graph.py                  # Interface Streamlit — matching interactif
├── offres_to_csv_spacy.py        # Pipeline NLP d'extraction des compétences/domaines
├── extract_roles_offres.py       # Extraction des rôles depuis les offres
├── database/
│   └── appetencia.db.dump        # Dump Neo4j de la base de données
├── requirements.txt              # Dépendances Python
└── README.md
```

---

## Prérequis

- Python 3.10+
- Neo4j Desktop avec **Neo4j 5.x** installé
- pip

---

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/uvsq22407865/Appetencia_project.git
cd Appetencia_project
```

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

Installer également le modèle spaCy français :

```bash
python -m spacy download fr_core_news_sm
```

### 3. Restaurer la base Neo4j

1. Ouvrir **Neo4j Desktop**
2. Créer une nouvelle instance locale
3. Créer une base nommée **`appetencia.db`**
4. **Arrêter** la base
5. Cliquer sur les `...` → **"Load database from file"** et sélectionner `database/appetencia.db.dump`

Ou via le terminal Neo4j :

```bash
bin/neo4j-admin database load appetencia.db --from-path=./database/ --overwrite-destination=true
```

6. **Redémarrer** la base

### 4. Lancer l'interface

```bash
streamlit run app_graph.py
```

---

## Connexion Neo4j

Au lancement, renseigner dans la barre latérale :

| Champ | Valeur |
|---|---|
| NEO4J_URI | `bolt://localhost:7687` |
| NEO4J_USER | `neo4j` |
| NEO4J_PASSWORD | taous2001|
| NEO4J_DB | `appetencia.db` |

---

## Fonctionnalités

- **Mode Offre → Candidats** : pour une offre donnée, retrouve les candidats les plus compatibles
- **Mode Candidat → Offres** : pour un candidat donné, retrouve les offres les plus pertinentes
- **Score multi-dimensionnel** : rôles aspirés, domaines, intérêts, compétences
- **Coefficients paramétrables** : sliders α, β, γ, δ normalisés automatiquement (somme = 1)
- **Visualisation graphe interactif** avec code couleur par score
- **Décomposition détaillée** du score par pilier

---

## Pipeline NLP des offres

Pour réexécuter l'extraction d'information sur de nouvelles offres :

```bash
# Extraction des compétences et domaines
python offres_to_csv_spacy.py

# Extraction des rôles
python extract_roles_offres.py
```

Les offres doivent être placées sous forme de fichiers `.txt` dans le dossier `offres_text/`.

---

## Dépendances principales

```
streamlit
neo4j
pandas
pyvis
spacy
rapidfuzz
```

---

## Formule de score

$$\text{score} = \alpha \cdot \text{role\_score} + \beta \cdot \text{domain\_score} + \gamma \cdot \text{interest\_strength} + \delta \cdot \text{skills\_score}$$

Avec $\alpha + \beta + \gamma + \delta = 1$ (normalisés automatiquement).

Valeurs par défaut : Rôles 40% · Domaines 25% · Intérêts 20% · Compétences 15%
