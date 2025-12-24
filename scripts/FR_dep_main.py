import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
import glob
import json
import csv
import pandas as pd

# === CONFIGURATION ===
base_url = 'https://data.assemblee-nationale.fr'
page_url = 'https://data.assemblee-nationale.fr/acteurs/historique-des-deputes'
zip_download_dir = r"D:\Downloads"
zip_extract_dir = r"C:\Users\HONOR\Desktop\RA\France\data"
output_csv = os.path.join(zip_extract_dir, "FR_dep_combined.csv")
combined_json_path = os.path.join(zip_extract_dir, "combined_filtered.json")

# === ORGANE TYPES TO KEEP ===
valid_type_organe = {"BUREAU", "ASSEMBLEE"}
# e.g. Add "COMPER", "GP", "ORGEXTPARL" to get more mandate types

# === STEP 1: FETCH ZIP LINK FROM WEBPAGE ===
print("Fetching the webpage...")

response = requests.get(page_url, timeout=30)
response.raise_for_status()
soup = BeautifulSoup(response.text, 'lxml')

zip_link_tag = soup.find('a', href=lambda x: x and x.endswith('.json.zip'))
if not zip_link_tag:
    raise Exception("No .json.zip link found on the page.")

zip_url = urljoin(base_url, zip_link_tag['href'])
zip_filename = os.path.basename(zip_url)
zip_path = os.path.join(zip_download_dir, zip_filename)

print(f"Found ZIP URL: {zip_url}")
print(f"Downloading to: {zip_path}")

# === STEP 2: DOWNLOAD ZIP ===
with requests.get(zip_url, stream=True) as r:
    r.raise_for_status()
    with open(zip_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

print("Download completed.")

# === STEP 3: UNZIP CONTENTS ===
print(f"Extracting to: {zip_extract_dir}")
os.makedirs(zip_extract_dir, exist_ok=True)

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(zip_extract_dir)

print("Extraction completed.")

# === STEP 4: READ JSON FILES AND CREATE UNIFIED JSON LIST ===
def safe_get_string(value):
    #Return a cleaned string or None if the value is not a string
    return value.strip() if isinstance(value, str) else None

acteur_dir = os.path.join(zip_extract_dir, "json", "acteur")
if not os.path.isdir(acteur_dir):
    raise FileNotFoundError(f"Folder not found: {acteur_dir}")

json_files = glob.glob(os.path.join(acteur_dir, "*.json"))
print(f"Found {len(json_files)} JSON files to process...")

combined_data = []

for file_path in json_files:
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
    except Exception as e:
        print(f"Skipping {file_path} due to error: {e}")
        continue

    acteur = data.get("acteur", {})
    etat_civil = acteur.get("etatCivil", {})
    ident = etat_civil.get("ident", {})
    info_naissance = etat_civil.get("infoNaissance", {})
    profession = acteur.get("profession", {})
    soc_proc = profession.get("socProcINSEE", {})

    nom = safe_get_string(ident.get("nom", ""))
    prenom = safe_get_string(ident.get("prenom", ""))
    civ = safe_get_string(ident.get("civ", ""))
    date_naissance = safe_get_string(info_naissance.get("dateNais", ""))
    lieu_naissance = safe_get_string(info_naissance.get("villeNais", ""))
    dep_naissance = safe_get_string(info_naissance.get("depNais", ""))
    profession_libelle = safe_get_string(profession.get("libelleCourant", ""))
    cat_soc_pro = safe_get_string(soc_proc.get("catSocPro", ""))
    fam_soc_pro = safe_get_string(soc_proc.get("famSocPro", ""))

    mandats = acteur.get("mandats", {}).get("mandat", [])

    for mandat in mandats:
        if not isinstance(mandat, dict):
            continue

        type_organe = safe_get_string(mandat.get("typeOrgane", "")).upper() if mandat.get("typeOrgane") else ""
        if type_organe not in valid_type_organe:
            continue

        entry = {
            "nom": nom,
            "prenom": prenom,
            "civ": civ,
            "dateNaissance": date_naissance,
            "lieuNaissance": lieu_naissance,
            "depNais": dep_naissance,
            "profession": profession_libelle,
            "catSocPro": cat_soc_pro,
            "famSocPro": fam_soc_pro,
            "uid": safe_get_string(mandat.get("uid")),
            "acteurRef": safe_get_string(mandat.get("acteurRef")),
            "legislature": safe_get_string(mandat.get("legislature")),
            "typeOrgane": type_organe,
            "dateDebut": safe_get_string(mandat.get("dateDebut")),
            "dateFin": safe_get_string(mandat.get("dateFin")),
            "libQualite": safe_get_string(mandat.get("infosQualite", {}).get("libQualite")),
            "organeRef": safe_get_string(mandat.get("organes", {}).get("organeRef"))
        }
        combined_data.append(entry)

# === STEP 5: SAVE COMBINED JSON (OPTIONAL) ===
with open(combined_json_path, 'w', encoding='utf-8-sig') as f_json:
    json.dump(combined_data, f_json, ensure_ascii=False, indent=2)

print(f"Combined JSON saved to: {combined_json_path}")

# === STEP 6: WRITE TO CSV ===
# Convert combined_data (list of dicts) to a pandas DataFrame
df = pd.DataFrame(combined_data)

# === RENAME COLUMNS ===

column_renames = {
    "nom": "Last Name",
    "prenom": "First Name",
    "civ": "Civil Status",
    "dateNaissance": "Date of Birth",
    "lieuNaissance": "Place of Birth",
    "depNais": "Birth Department",
    "profession": "Profession",
    "catSocPro": "Socio-Professional category",
    "famSocPro": "Socio-Professional family",
    "acteurRef": "Deputy ID",
    "legislature": "Legislature",
    "typeOrgane": "Type of Organe",
    "dateDebut": "Mandate Start Date",
    "dateFin": "Mandate End Date",
    "libQualite": "Position"
}

df.rename(columns=column_renames, inplace=True)

df['Sex'] = df['Civil Status'].str.lower().map({
    'mme': 'Female', 'm.': 'Male'
}).fillna('N/A')

#drop unwanted columns
columns_to_drop = ["uid", "organeRef"]
df.drop(columns=[col for col in columns_to_drop if col in df.columns], inplace=True)

date_columns = [
    "Date of Birth", "Mandate Start Date", "Mandate End Date"
]

for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')


desired_order = [
    "Last Name", "First Name", "Deputy ID", "Civil Status", "Sex",
    "Date of Birth", "Place of Birth", "Birth Department",
    "Profession", "Socioprofessional category", "Socioprofessional family",
    "Legislature", "Mandate Start Date", "Mandate End Date", "Type of Organe", "Position"
]

ordered_cols = [col for col in desired_order if col in df.columns]
df = df[ordered_cols + [col for col in df.columns if col not in ordered_cols]]

# === SAVE FINAL CSV ===
df.to_csv(output_csv, index=False, encoding="utf-8-sig")
print(f"Cleaned and renamed CSV saved at:\n{output_csv}")
