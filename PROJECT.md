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
└── enriched/   # Wikidata / Wikipedia enrichment, produced by src/03_enrich.py
```

Bash:

```bash
# Create the `data/` folder and its subfolders
mkdir -p data/raw data/clean data/enriched

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
"raw", "clean", "enriched" | ForEach-Object { New-Item -ItemType Directory -Force -Path "data/$_" | Out-Null }

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

### 3. Install the system prerequisites (once per machine)

#### 3.1 Python

Python 3.14 (the project is developed and tested with 3.14). Check with:

```bash
python --version
```

#### 3.2 Java 21

Spark runs on the Java Virtual Machine: PySpark 4.x needs **Java 17 or 21** and the `JAVA_HOME` environment variable.

Windows (PowerShell):

```powershell
winget install EclipseAdoptium.Temurin.21.JDK
```

macOS / Linux:

```bash
brew install openjdk@21              # macOS
sudo apt install openjdk-21-jdk      # Debian / Ubuntu
```

Close and reopen the terminal (and VS Code), then check that both commands print something:

```bash
java -version
echo $JAVA_HOME                      # PowerShell: echo $env:JAVA_HOME
```

If `JAVA_HOME` is empty, set it to the JDK installation folder, for example on Windows:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\<jdk-21 folder>", "User")
```

#### 3.3 Hadoop native binaries (Windows only)

On Windows, Spark needs `winutils.exe` and `hadoop.dll` to write files (Parquet). Without them, `src/01_clean.py` fails when saving with `HADOOP_HOME and hadoop.home.dir are unset`. These are community builds of the Hadoop Windows binaries ([cdarlint/winutils](https://github.com/cdarlint/winutils)).

```powershell
New-Item -ItemType Directory -Force C:\hadoop\bin | Out-Null
curl.exe -fL -o C:\hadoop\bin\winutils.exe https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe
curl.exe -fL -o C:\hadoop\bin\hadoop.dll https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";C:\hadoop\bin", "User")
```

Close and reopen the terminal (and VS Code) so that the new variables are taken into account.

#### 3.4 Docker (streaming only)

The streaming step runs Kafka in a Docker container. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it before running the streaming step. Check with :

```bash
docker --version
```

### 4. Set up the Python virtual environment

From the project root, create the virtual environment and install the pinned dependencies of `requirements.txt`:

Bash (macOS / Linux / Git Bash):

```bash
python -m venv .venv
source .venv/bin/activate            # Git Bash on Windows: source .venv/Scripts/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

If PowerShell refuses to run `Activate.ps1` ("running scripts is disabled on this system"), allow local scripts once, then activate again:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Check the installation:

```bash
python -c "import pyspark, pandas, pyarrow, requests; print('pyspark', pyspark.__version__, '| pandas', pandas.__version__)"
```

> **NOTE:** The virtual environment must be **activated in every new terminal** before running a script. The prompt then starts with `(.venv)`.

Paths and Spark settings (driver memory, shuffle partitions) are defined in `config.py`. Lower `SPARK_DRIVER_MEMORY` if the machine has less than 8 GB of RAM.

### 5. Run the pipeline

With the virtual environment activated, run each step from the project root, in this order:

| Step | Command | Output | Indicative duration |
|------|---------|--------|---------------------|
| Data preparation | `python src/01_clean.py` | `data/clean/*.parquet`, `reports/01_data_preparation.txt` | ~25 min |
| Data exploration | `python src/02_explore.py` | `reports/02_exploration.txt` | |
| Data enrichment | `python src/03_enrich.py` | `data/enriched/*.parquet`, `reports/03_enrichment.txt` | |
| Streaming | `python src/04_wiki_producer.py` (terminal 2) + `python src/04_streaming.py` (terminal 3), then `python src/04_summary_streaming.py` | Console output, `reports/04_streaming.txt`, `reports/04_summary_streaming.txt` | Runs until stopped (≥ 15 min) |

Each step reads the output of the previous ones: `src/02_explore.py` and `src/03_enrich.py` need `data/clean/` to be filled by `src/01_clean.py` first.

#### Streaming step

Open three terminals at the project root, with the virtual environment activated:

```bash
docker compose up -d                                            # 1. start Kafka (localhost:9092)
python src/04_wiki_producer.py                                  # 2. send Wikipedia edits to the topic `wiki-changes`
python -u src/04_streaming.py | tee reports/04_streaming.txt    # 3. Spark job : match edits with IMDb titles and people
```

The third command prints the results in the console and saves them in `reports/04_streaming.txt` at the same time. The first run downloads the Spark Kafka connector, so it needs internet. Let the job run for 15 to 30 minutes, then press Ctrl+C in both Python terminals and stop Kafka:

```bash
docker compose down
```

The raw output contains many empty batches. To keep only the matching events and the final count of each window (the tables shown in `REPORT.md`), run:

```bash
python src/04_summary_streaming.py reports/04_streaming.txt > reports/04_summary_streaming.txt
```

## AI & LLM tools usage
