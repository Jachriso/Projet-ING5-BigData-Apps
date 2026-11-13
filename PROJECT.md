# Big Data Processing and Application Gr01 ING5
Group : _gr-01 ING5_  

## Project contributors 
Member 1 : _PARRILLA Jade_
- jade.parrilla@edu.ece.fr
- jachriso

Member 2 : _FAUTRAD Annabelle_ 
- annabelle.fautrad@edu.ece.fr
- annaftrd

## Instructions

### 1. Clone the repository

```bash
git clone https://github.com/Jachriso/Projet-ING5-BigData-Apps.git
cd Projet-ING5-BigData-Apps
```

### 2. Set up the local data folder

The datasets are **not versioned** (the `data/` folder is listed in `.gitignore`). Each member rebuilds the same local data base by running the commands below in a terminal, **from the project root**. Use the **Bash** version on macOS / Linux / Git Bash, or the **PowerShell** version on Windows.

The resulting layout is:

```text
data/
├── raw/        # Raw IMDb files (.tsv.gz), never modified
├── clean/      # Cleaned data (Parquet), produced by src/01_clean.py
├── enriched/   # Wikidata / Wikipedia enrichment, produced by src/02_enrich.py
└── gold/       # Final analysis tables, produced by src/03_transform.py
```

Bash:

```bash
# Create the `data/` folder and its subfolders
mkdir -p data/raw data/clean data/enriched data/gold

# Download the raw IMDb datasets into `data/raw/`
for f in name.basics title.akas title.basics title.crew title.episode title.principals title.ratings; do
  out="data/raw/$f.tsv.gz"
  if [ -f "$out" ]; then echo "SKIP  $f"; continue; fi
  curl -fL -o "$out.part" "https://datasets.imdbws.com/$f.tsv.gz" && mv "$out.part" "$out"
done

# Check that everything is in place
ls -lh data/raw
```

PowerShell:

```powershell
# Create the `data/` folder and its subfolders
"raw", "clean", "enriched", "gold" | ForEach-Object { New-Item -ItemType Directory -Force -Path "data/$_" | Out-Null }

# Download the raw IMDb datasets into `data/raw/`
$files = "name.basics", "title.akas", "title.basics", "title.crew", "title.episode", "title.principals", "title.ratings"
foreach ($f in $files) {
    $out = "data/raw/$f.tsv.gz"
    if (Test-Path $out) { Write-Host "SKIP  $f"; continue }
    curl.exe -fL -o "$out.part" "https://datasets.imdbws.com/$f.tsv.gz"
    if ($LASTEXITCODE -eq 0) { Move-Item "$out.part" $out }
}

# Check that everything is in place
Get-ChildItem data/raw | Select-Object Name, @{Name = "MB"; Expression = { [math]::Round($_.Length / 1MB, 1) }}
```


## AI & LLM tools usage