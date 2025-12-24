from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


#CHANGE THIS PATH BEFORE RUNNING THE CODE
#install the libraries if not installed
#pip install selenium beautifulsoup4 pandas requests lxml

output_path = r"C:\Users\HONOR\Desktop\RA\France\data\FR_dep11_full.csv"

#setting up the selenium
options = Options()
options.add_argument("--headless")
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

#1. SCRAP THE MAIN INFORMATION FROM THE TABLE
#looking for the link to the multicriterial search of deputies of 11th legislature

try:
    print("Scraping table data...")
    driver.get("https://www.assemblee-nationale.fr/qui/index.asp?legislature=11")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//a[contains(text(),'Recherche multicritère')]"))
    )
    driver.execute_script("document.Lien5.submit()")

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.NAME, "id_acteur"))
    )

    #selecting all checkboxes
    driver.execute_script("""
        var checkboxes = document.querySelectorAll("input[type='checkbox']");
        checkboxes.forEach(cb => cb.checked = true);
    """)

    #clicking search to obtain the results
    driver.find_element(By.XPATH, "//input[@type='submit' and @value='Afficher les résultats']").click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "tablesorter0"})

    #in the obtained table look for headers and rows
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    rows = []
    for tr in table.find("tbody").find_all("tr"):
        cells = [td.get_text(strip=True).replace("\xa0", " ") for td in tr.find_all("td")]
        if cells:
            rows.append(cells)
    #input information into data frame
    df_table = pd.DataFrame(rows, columns=headers)

    #cleanning from the unnecessary columns (such as Link to personal webpage, Age category and Age as it's not updated)
    #add the Sex column mapping the Civil Status
    df_table = df_table.drop(columns=["Lien fiche", "Catégorie d'âge", "Age"], errors="ignore")
    df_table['Sex'] = df_table['Civilite'].str.lower().map({'mme': 'Female', 'm.': 'Male'}).fillna('N/A')

    #creating Full Name column to join on
    df_table['FullName'] = (df_table['Prénom'].str.strip() + ' ' + df_table['Nom'].str.strip()).str.strip()

except Exception as e:
    print(f"Selenium error: {e}")
    driver.save_screenshot("errore_screenshot.png")
    driver.quit()
    raise

finally:
    driver.quit()

#2. SCRAP THE INFORMATION FROM INDIVIDUAL PROFILE
#as in the scrapped table there's no start date and end date for mandate look for this on personal profiles

print("Scraping individual profiles...")
list_url = 'https://www.assemblee-nationale.fr/qui/xml/liste_alpha.asp?legislature=11'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

session = requests.Session()
session.headers.update(headers)

retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

resp = session.get(list_url, timeout=20)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, 'lxml')

#collecting (name, profile_url)
profiles = []
for tag in soup.find_all('a', href=True):
    if "fiches_id" in tag['href']:
        for span in tag.find_all('span'):
            span.extract()
        raw_name = tag.get_text(strip=True)
        #using regular exression to find the simular lines
        name = re.sub(r"^(M(?:me)?\.?|MM\.?|AM)\s+", "", raw_name)  # remove title
        profile_url = urljoin(list_url, tag['href'])
        profiles.append((name, profile_url))

#parsing every profile
def fetch_profile(profile):
    name, url = profile
    try:
        resp = session.get(url, headers={"Referer": list_url}, timeout=20)
        resp.raise_for_status()
        soup_profile = BeautifulSoup(resp.text, 'lxml')

        #looking for "born" and extracting the place of birth
        pob = "N/A"
        for p in soup_profile.find_all('p'):
            if 'Né' in p.text or 'Née' in p.text:
                try:
                    pob = p.get_text(strip=True).split(' à ', 1)[1].strip()
                except IndexError:
                    pass
                break

        #looking for the section "Mandates in National Assembly" and for its parent tag
        start_date = end_date = ""
        mandat_header = soup_profile.find('b', string=re.compile(r"MANDAT À L'ASSEMBLÉE NATIONALE", re.I))
        if mandat_header:
            ancestor = mandat_header
            for _ in range(4):
                ancestor = ancestor.find_parent()
                if not ancestor:
                    break
            #after finding the parent tag look for the ul tag and find inside the p tags
            #if find look for the line of certain pattern using regular expression and extracting the dates of mandate in 11th leguslature
            next_ul = ancestor.find_next_sibling('ul') if ancestor else None
            if next_ul:
                full_text = "\n".join(p.get_text(strip=True) for p in next_ul.find_all('p'))
                start_match = re.search(r"date de début de mandat\s*:\s*(\d{2}/\d{2}/\d{4})", full_text, re.I)
                end_match = re.search(r"fin du mandat au\s*:\s*(\d{2}/\d{2}/\d{4})", full_text, re.I)
                if start_match:
                    start_date = start_match.group(1)
                if end_match:
                    end_date = end_match.group(1)

        time.sleep(1)

        #returning the extracting data
        return {
            'Name': name,
            'Place of Birth': pob,
            'Mandate Start Date': start_date,
            'Mandate End Date': end_date
        }

    except Exception as e:
        print(f"Error with {url}: {e}")
        return None

#to make information extracture faster apply parallel scraping
deputies_data = []
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_profile = {executor.submit(fetch_profile, p): p for p in profiles}
    for future in as_completed(future_to_profile):
        result = future.result()
        if result:
            deputies_data.append(result)

df_profiles = pd.DataFrame(deputies_data)

#3. JOINING BOTH DATAFRAMES
print("Merging table and profiles...")

#create merge key: from 1st data frame use the "Surname" and "Name" columns united. from the 2nd "Name"
df_table['__merge_key'] = df_table['Prénom'].str.strip() + ' ' + df_table['Nom'].str.strip()

#merge on key
df_merged = pd.merge(df_table, df_profiles, how='left', left_on='__merge_key', right_on='Name')

#drop all helper columns related to the merge to avoid redundant information in data set
df_merged = df_merged.drop(columns=[col for col in ['__merge_key', 'Name', 'FullName'] if col in df_merged.columns])

#4. REORDER COLUMNS
column_renames = {
    "Prénom": "First Name",
    "Nom": "Last Name",
    "Civilite": "Civil Status",
    "Groupe": "Political Group",
    "Région d'élection": "Electoral Region",
    "N° circ.": "Constituency Number",
    "Commission permanente": "Standing Committee",
    "Profession": "Profession",
    "Catégorie socioprofessionnelle": "Socio-Professional category",
    "Famille socioprofessionnelle": "Socio-Professional family",
    "Date de naissance": "Date of Birth",
    "Conseil municipal": "Municipal Council",
    "Conseil régional": "Regional Council",
    "Autre mandat local": "Other Local Mandate",
    "Département d'élection": "Electoral Department",
    "Mandat communal": "Municipal Mandate",
    "Conseil départemental": "Departmental Council",
    "Mandat départemental": "Departmental Mandate",
    "Mandat régional": "Regional Mandate"
}

df_merged.rename(columns=column_renames, inplace=True)

desired_order = [
    "Last Name", "First Name", "Civil Status", "Sex", "Date of Birth", "Place of Birth",
    "Political Group", "Mandate Start Date", "Mandate End Date", "Electoral Region", "Constituency Number", "Electoral Department",
    "Standing Committee", "Profession", "Socio-Professional category", "Socio-Professional family",
     "Departmental Council", "Regional Council", "Municipal Council", "Departmental Mandate", "Regional Mandate", "Municipal Mandate", "Other Local Mandate"  
]

existing_columns = [col for col in desired_order if col in df_merged.columns]
df_merged = df_merged[existing_columns + [col for col in df_merged.columns if col not in existing_columns]]

#5. SAVE AS CSV
df_merged.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"Final dataset saved: {output_path}")
