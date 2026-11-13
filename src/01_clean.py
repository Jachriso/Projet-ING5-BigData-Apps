# 01_clean.py
# Étape 1 — Nettoyage : lit data/raw/, remplace les "\N" par null, corrige les types,
# découpe les colonnes listes (genres, primaryProfession, knownForTitles, directors...)
# et écrit le résultat en Parquet dans data/clean/.
