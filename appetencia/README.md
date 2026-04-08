# 🧠 Appetencia --- Documentation du moteur de Matching

## 🎯 Objectif

Ce document explique en détail comment est calculé le score de matching
entre : - un **Candidat** - une **Offre d'emploi**

Le modèle repose sur trois piliers fondamentaux :

1.  🧩 Les **Compétences** (alignement technique)
2.  🏷️ Les **Domaines** (alignement stratégique / métier)
3.  ❤️ Les **Intérêts personnels** (motivation intrinsèque)

------------------------------------------------------------------------

# 1️⃣ MATCHING DES COMPÉTENCES

## Structure du graphe

Candidat → Anecdote → MONTRE {groupe} → Compétence\
Offre → DEMANDE → Compétence

### Pondération des groupes

  Groupe     Signification                                Poids
  ---------- -------------------------------------------- -------
  RESULTAT   Compétence prouvée avec résultat mesurable   1.0
  MAITRISE   Bonne maîtrise                               0.7
  PLAISIR    Appréciée mais peu démontrée                 0.4
  Autre      Valeur neutre                                0.5

------------------------------------------------------------------------

## Métriques calculées

### overlap

Nombre de compétences communes entre le candidat et l'offre.

### nbReq

Nombre total de compétences demandées par l'offre.

### coverage

coverage = overlap / nbReq

Mesure la proportion de l'offre couverte par le candidat.

### group_score

Moyenne des poids des groupes sur les compétences communes.

### skills_score

skills_score = 0.75 × coverage + 0.25 × group_score

75% = adéquation technique\
25% = qualité de démonstration

------------------------------------------------------------------------

# 2️⃣ MATCHING DES DOMAINES

## Structure du graphe

Candidat → INTERESSE_PAR {type:I\|A} → Domaine\
Offre → A_POUR_DOMAINE {confidence} → Domaine

### Pondération côté candidat

  Type   Signification   Poids
  ------ --------------- -------
  I      Intérêt fort    1.0
  A      Attirance       0.6

confidence ∈ \[0,1\]

### domain_score

Pour chaque domaine commun : contribution = poids_candidat ×
confidence_offre

domain_score = moyenne(contribution)

------------------------------------------------------------------------

# 3️⃣ FORCE DES INTÉRÊTS PERSONNELS

Candidat → A_POUR_INTERET {type:N\|I\|A\|P} → Intérêt

  Type   Signification              Poids
  ------ -------------------------- -------
  N      Nourrissant profondément   1.0
  I      Important                  0.8
  A      Attirant                   0.5
  P      Pas pour moi               -1.0

interest_strength = (moyenne + 1) / 2

------------------------------------------------------------------------

# 🧮 SCORE FINAL

score = 0.60 × skills_score + 0.30 × domain_score + 0.10 ×
interest_strength

------------------------------------------------------------------------

## 📊 Exemple

coverage = 0.667\
group_score = 0.7\
skills_score = 0.675\
domain_score = 0.76\
interest_strength = 0.831

score ≈ 0.716

------------------------------------------------------------------------

## 🔎 Principe global

Le matching combine : - Adéquation technique - Alignement métier -
Motivation intrinsèque

Ce modèle rend le matching plus intelligent, explicable et cohérent.
