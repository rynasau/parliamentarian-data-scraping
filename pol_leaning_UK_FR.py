import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
from io import StringIO
import re


output_path = r"C:\Users\HONOR\Desktop\RA\France\pol_leaning\ches_parties_UK_FR.csv"

print("Starting CHES data processing...")

# CHES dataset page
url = 'https://www.chesdata.eu/ches-europe'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "lxml")

csv_links = []

# Filter CSV links by year and content
for link in soup.find_all('a', href=True):
    href = link['href']
    if not href.lower().endswith('.csv'):
        continue
    if '1999-2019' in href:
        csv_links.append(urljoin(url, href))
    elif '2024' in href and 'Ukraine' not in href:
        csv_links.append(urljoin(url, href))
    elif '2017' in href and 'combined_experts' not in href:
        csv_links.append(urljoin(url, href))

# Download and parse CSVs
dataframes = {}
for link in csv_links:
    try:
        print(f"Downloading: {link}")
        csv_response = requests.get(link)
        csv_response.raise_for_status()
        df = pd.read_csv(StringIO(csv_response.text))
        dataframes[link] = df
    except Exception as e:
        print(f"Failed to load {link}: {e}")

# Family ID to name mapping
family_mapping = {
    1: "Radical Right",
    2: "Conservatives",
    3: "Liberal",
    4: "Christian-Democratic",
    5: "Socialist",
    6: "Radical Left",
    7: "Green",
    8: "Regionalist",
    9: "No family",
    10: "Confessional",
    11: "Agrarian/Center"
}

# Mapping for party names by party_id
party_name_mapping = {
    601: ("PCF", "Parti Communiste Français"),
    602: ("PS", "Parti Socialiste"),
    603: ("PRG", "Parti Radical de Gauche"),
    605: ("VERTS; EELV", "Les Verts; Europe Écologie Les Verts"),
    609: ("RPR; UMP; LR", "Rassemblement pour la République; Union pour un Mouvement Populaire; Les Républicains"),
    610: ("FN; RN", "Front National; Rassemblement national"),
    612: ("RPF/MPF;MPF", "Rassemblement pour la France/Mouvement Pour la France;  Mouvement Pour la France"),
    613: ("UDF; MoDem", "Union pour la Démocratie Française; Mouvement Démocrate"),
    614: ("LO-LCR", "Lutte Ouvrière/Ligue communiste révolutionnaire"),
    615: ("DL", "Démocratie Libérale"),
    617: ("MEI", "Mouvement Ecologiste Indépendant"),
    618: ("D", "La Droite"),
    619: ("CPNT", "Chasse, Pêche, Nature, Traditions"),
    620: ("MN", "Mouvement National Républicain"),
    621: ("NC", "Nouveau Centre"),
    622: ("PRV", "Parti radical"),
    623: ("AC", "Alliance centriste"),
    624: ("PG", "Parti de Gauche"),
    625: ("Ens", "Ensemble"),
    626: ("RE; REM", "Rennaissance; La République En Marche"),
    627: ("FI", "La France Insourmise"),
    628: ("DLF", "Debout la France"),
    630: ("REC", "Reconquête"),
    631: ("Horizons", "Horizons"),


    1101: ("Cons", "Conservative Party"),
    1102: ("Lab", "Labour Party"),
    1104: ("LibDem", "Liberal Democratic Party"),
    1105: ("SNP", "Scottish National Party"),
    1106: ("Plaid", "Plaid Cymru"),
    1107: ("Green", "Green Party"),
    1108: ("UKIP", "United Kingdom Independence Party"),
    1109: ("BNP", "British National Party"),
    1110: ("Brexit; REF UK", "Brexit Party; Reform UK"),
    1150: ("SF", "Sinn Féin"),
    1151: ("DUP", "Democratic Unionist Party"),
}

def map_family(value):
    try:
        key = int(value)
        return family_mapping.get(key, value)
    except:
        match = re.match(r"^\s*(\d+)\s*\.\s*", str(value).strip(), re.IGNORECASE)
        if match:
            num = int(match.group(1))
            return family_mapping.get(num, value)
        else:
            return str(value).strip().capitalize()

# Process both France and UK
filtered_dfs = []

for url, df in dataframes.items():
    print(f"Filtering: {url}")
    try:
        if df['country'].dtype == object:
            df_filtered = df[df['country'].str.upper().isin(['FR', 'UK'])]
        else:
            df_filtered = df[df['country'].isin([6, 11])]

        # Handle missing year in 2024 CSV
        if 'CHES_2024_final_v2.csv' in url:
            df_filtered = df_filtered.copy()
            df_filtered['year'] = 2024

        selected_columns = ['country', 'year', 'party_id', 'family', 'lrgen', 'galtan']
        df_filtered = df_filtered[[col for col in selected_columns if col in df_filtered.columns]]

        # Normalize family names
        if 'family' in df_filtered.columns:
            df_filtered['family'] = df_filtered['family'].apply(map_family)

        filtered_dfs.append(df_filtered)
    except Exception as e:
        print(f"Error filtering {url}: {e}")

# Combine all filtered data
print("\nCombining filtered datasets...")
combined_df = pd.concat(filtered_dfs, ignore_index=True)

# Replace numeric country codes
combined_df['country'] = combined_df['country'].replace({6: 'fr', 11: 'uk'})

# Fill missing year with 2024
combined_df['year'] = combined_df['year'].fillna(2024)

# Drop rows with missing party_id
combined_df = combined_df.dropna(subset=['party_id'])

# Convert types
combined_df['year'] = combined_df['year'].astype(int)
combined_df['party_id'] = combined_df['party_id'].astype(int)

# Add party_abb and party_name columns
combined_df['party_abb'] = combined_df['party_id'].apply(lambda pid: party_name_mapping.get(pid, ("", ""))[0])
combined_df['party_name'] = combined_df['party_id'].apply(lambda pid: party_name_mapping.get(pid, ("", ""))[1])

# Sort
combined_df_sorted = combined_df.sort_values(by=['country', 'party_id', 'year']).reset_index(drop=True)
column_order = ['country', 'year', 'party_id', 'party_abb', 'party_name', 'family', 'lrgen', 'galtan']
combined_df_sorted = combined_df_sorted[column_order]
# Save
print(f"Saving combined data to: {output_path}")
combined_df_sorted.to_csv(output_path, index=False, encoding="utf-8-sig")

print("Data saved successfully.")

#2024 Chapel Hill expert survey
#1999-2019 Chapel Hill Expert Survey (CHES) trend file 2002, 2006, 2010, 2014, 2019.
#2017 Chapel Hill Expert FLASH Survey (CHES)
