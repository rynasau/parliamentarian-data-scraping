from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
from urllib.parse import urljoin, quote


#before running change the path for saving the results (line 251)
output_path = r'C:\Users\HONOR\Desktop\RA\Brazil\data\br_parties_data.csv'
start_url = 'https://www25.senado.leg.br/web/senadores/legislaturas-anteriores/-/a'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
legislatures = [51, 52, 53, 54, 55, 56]

parties_data = []
seen_parties = set()

def safe_get_text(tags, index):
    try:
        return tags[index].get_text(strip=True)
    except IndexError:
        return "N/A"

session = requests.Session()
session.headers.update(headers)

for leg in legislatures:
    print(f"Processing legislature {leg}ª...")
    party_url = f"{start_url}/{leg}/por-partido"

    response_p = session.get(party_url)
    soup_p = BeautifulSoup(response_p.text, 'lxml')

    result_p = soup_p.find('table', class_='table', id = 'senadoreslegislaturasanteriores-tabela-senadores')
    if result_p:
        parties = [
            party for party in result_p.find('tbody').find_all('tr', class_='search-group-row')
        ]
        for party in parties:
            party_info = party.get_text(strip=True)
            split_parts = party_info.split(' - ', 1)

            if len(split_parts) != 2:
                continue

            party_abb = split_parts[0].strip()
            party_name = split_parts[1].strip()

            if (party_abb, party_name) in seen_parties:
                continue
            seen_parties.add((party_abb, party_name))

            parties_data.append({
                "Party Abbreviation": party_abb,
                "Party Full Name": party_name,
            })


    else:
        print(f"Table not found for legislature {leg}. Page structure may have changed.")
    time.sleep(1)

df = pd.DataFrame(parties_data)
df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"Data saved to CSV. Total rows: {len(df)}")
