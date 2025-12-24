import requests
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin


output_path = r"C:\Users\HONOR\Desktop\RA\France\data\FR_dep17_full.csv"

#1. DOWNLOAD the CSV with current deputies data from data.gouv.fr
url = 'https://www.data.gouv.fr/datasets/deputes-actifs-de-lassemblee-nationale-informations-et-statistiques/'
print("Fetching the webpage...")
response = requests.get(url, timeout=30)
response.raise_for_status()
soup = BeautifulSoup(response.text, 'lxml')

csv_tag = soup.find('a', href=lambda x: x and x.startswith('https://www.data.gouv.fr/api'))
if not csv_tag:
    raise Exception("No .json.zip link found on the page.")

csv_url = urljoin(url, csv_tag['href'])
print(f"Found CSV URL: {csv_url}")

resp_csv = requests.get(csv_url, timeout=30)
response.raise_for_status()

df_gouv = pd.read_csv(StringIO(resp_csv.content.decode('utf-8')), sep=",", encoding='utf-8')
#renaming and reordering columns
gouv_renames = {
    "id": "Deputy ID", "legislature": "Legislature",
    "civ": "Civil Status", "nom": "Last Name", "prenom": "First Name", "villeNaissance": "Place of Birth",
    "naissance": "Date of Birth", "age": "Age", "groupe": "Group", "groupeAbrev": "Group Abbreviation",
    "departementNom": "Department", "circo": "Constituency",
    "datePriseFonction": "Start Date of Current Mandate", "job" : "Profession", "nombreMandats": "Number of Parliamentary Terms",
    "experienceDepute": "Parliamentary Experience (days/months/years)",
    "scoreParticipation": "Participation Score", "scoreParticipationSpecialite": "Speciality Participation Score",
    "scoreLoyaute": "Loyalty Score", "scoreMajorite": "Proximity to Majority",
    "dateMaj": "Last Update"
}

df_gouv.rename(columns=gouv_renames, inplace=True)
df_gouv["Sex"] = df_gouv["Civil Status"].str.lower().map({"mme": "Female", "m.": "Male"}).fillna("N/A")

gouv_order = [
    "Deputy ID", "Legislature", "Last Name", "First Name", "Civil Status", "Sex", "Place of Birth", "Date of Birth", "Age",
    "Group", "Group Abbreviation", "Department", "Constituency", "Start Date of Current Mandate"
]
ordered = [c for c in gouv_order if c in df_gouv.columns]
df_gouv = df_gouv[ordered + [c for c in df_gouv.columns if c not in ordered]]

#2. SCRAPING ADDITIONAL INFORMATION ABOUT CURRENT DEPUTIES

driver = webdriver.Chrome()
driver.get("https://www2.assemblee-nationale.fr/deputes/recherche-multicritere")
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "infosGenFiche")))
checkbox_names = [
    "infosGenNom", "infosGenPrenom", "infosGenCiv", "infosGenGroupe",
    "infosGenDept", "infosGenRegion", "infosGenCirc", "infosGenComper",
    "infosGenCatSocio", "infosGenFamSocio", "infosGenDateNaiss"
]

for name in checkbox_names:
    try:
        cb = driver.find_element(By.NAME, name)
        if not cb.is_selected():
            cb.click()
    except:
        pass
WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, "//button[@type='submit']")))
driver.find_element(By.XPATH, "//button[@type='submit']").click()
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table")))
tbl_html = driver.find_element(By.XPATH, "//table").get_attribute('outerHTML')
driver.quit()

df_web = pd.read_html(StringIO(tbl_html))[0]

# Optionally rename columns to match
df_web.rename(columns={"Prénom" : "First Name", "Nom" :"Last Name", "Civilite" : "Civil Status"}, inplace=True)

#3. MERGING BOTH DATA FRAMES ON COLUMNS NAME
df_web["__merge_key"] = df_web["Last Name"].str.strip() + " " + df_web["First Name"].str.strip()
df_gouv["__merge_key"] = df_gouv["Last Name"].str.strip() + " " + df_gouv["First Name"].str.strip()

columns_to_add = [
    "__merge_key",
    "Région",
    "Commission",
    "Cat. socioprof.",
    "Fam. socioprof."
]
df_web_subset = df_web[columns_to_add]

df_merged = pd.merge(
    df_gouv,
    df_web_subset,
    how="left",
    on="__merge_key"
)

df_merged = df_merged.drop(columns=[col for col in ["__merge_key", "mail", "twitter", "facebook", "website"] if col in df_merged.columns])

#also dropping columns with links to socials


column_renames = {
    "Région": "Region",
    "Cat. socioprof.": "Socio-Professional category",
    "Fam. socioprof.": "Socio-Professional family"
    }

df_merged.rename(columns=column_renames, inplace=True)

date_columns = [
    "Date of Birth", "Start Date of Current Mandate", "Last Update"
]

for col in date_columns:
    if col in df_merged.columns:
        df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce').dt.strftime('%d/%m/%Y')

# Final tweaks and save
print(df_merged.head())
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print("Merged dataset saved as 'FR_dep17_full.csv'")
