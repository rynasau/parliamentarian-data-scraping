from bs4 import BeautifulSoup
import requests
import time
import pandas as pd
from urllib.parse import urljoin, quote

###CHANGE THIS PATH BEFORE RUNNING THE CODE###
output_path = r'C:\Users\HONOR\Desktop\RA\Brazil\data\br_senate.csv'

#setting up the code: url's and headers
main_url = 'https://www6g.senado.leg.br/busca/?colecao=Senadores'
alt_url = 'https://www25.senado.leg.br/web/senadores/legislaturas-anteriores/-/a'
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

#from the main_url extract the prsonal and professional information about politicians
#from alt_url collect info politician - political party, party abbreviation - party full name

legislatures_main = [51, 52, 53, 54, 55, 56, 57, 58]
legislatures_alt = [51, 52, 53, 54, 55, 56]

#storage variables
senators_data = []
seen_urls = {}
short_name_party_map = {}
party_fullname_map = {}

#to ensure that senators who served during many legislatures are not duplicated create seen_urls
#to ensure that each party name and abbreviation is collected only once create seen_parties
#to map short name of senator from the main_url with his/her short name from the alt_url create short_name_party_map

def safe_get_text(tags, index):
    try:
        return tags[index].get_text(strip=True)
    except IndexError:
        return "N/A"

session = requests.Session()
session.headers.update(headers)

###1. SCRAPPING ALL BASIC INFO ABOUT SENATORS FROM THE MAIN_URL

for leg in legislatures_main:
    print(f"Processing legislature {leg}ª...")
    encoded_leg = quote(f"{leg}ª Legislatura")

#going through all legislatures and all pages

    for p in range(1,17):
        print(f"Processing page {p} of legislature {leg}...")
        page_url = f"{main_url}&legislatura={encoded_leg}&p={p}"
        response = session.get(page_url)
        if response.status_code != 200:
            print(f'Request failed at page {p} (legislature {leg}) with status code {response.status_code}')
            break

        soup = BeautifulSoup(response.text, 'lxml')
        results = soup.find_all('div', class_='sf-busca-resultados-item')
        valid_results = [r for r in results if r.find('h3') and r.find('a', href=True)]
        if not valid_results:
            print("No more senators found")
            break

#on the initial webpage find all the links to senators personal profiles and follow these links by turn, keeping track of them

        for result in valid_results:
            try:
                link_tag = result.find('h3').find('a', href=True)
                profile_url = urljoin(main_url, link_tag["href"])
                if profile_url in seen_urls:
                    seen_urls[profile_url].add(leg)
                    continue
                seen_urls[profile_url] = {leg}

#looking through the text on each senator's block identifying the sex

                main_block = result.get_text(separator=' ', strip=True).lower()
                if 'senadora' in main_block:
                    sex = "Female"
                elif 'senador' in main_block:
                    sex = "Male"
                else:
                    sex = "N/A"

                profile_response = session.get(profile_url)
                profile_soup = BeautifulSoup(profile_response.text, "lxml")

                personal_info = profile_soup.find('dl', class_='dl-horizontal')
                info_tags = personal_info.find_all('dd') if personal_info else []

#finding the block with personal info and extracting from it full name, date of birth and place of birth

                f_name = safe_get_text(info_tags, 0)
                dob = safe_get_text(info_tags, 1)
                pob = safe_get_text(info_tags, 2)
                #office = safe_get_text(info_tags, 3)
                #phone = safe_get_text(info_tags, 4)
                #mail = safe_get_text(info_tags, 5)

#from the head block collecting short name of senator

                head_div = profile_soup.find('div', class_='head')
                name = head_div.find('h1').get_text(strip=True).split(" -")[0] if head_div else "N/A"

#collecting information about parties (were available), otherwise put "N/A"
#information about political party is available only for sitting senators, we'll fix this later using alt_url

                party_tag = profile_soup.find('small').get_text(strip=True) if profile_soup.find('small') else ''
                parts = [p.strip() for p in party_tag.split(' - ')]
                party = parts[1].split('(')[0].strip() if len(parts) > 1 else "N/A"

#identifying position in party (were available), otherwise put "N/A"

                if ' (Fora de Exercício) ' in profile_soup.text:
                    position = "N/A"
                elif 'Líder' in party_tag:
                    position = "Leader"
                elif '1° Vice-líder' in party_tag:
                    position = "1st Vice-leader"
                elif '2° Vice-líder' in party_tag:
                    position = "2nd Vice-leader"
                else:
                    position = "Member"

#identifying whether senator is sitting now or out of service

                status = "Out of Service" if ' (Fora de Exercício) ' in profile_soup.text else "Sitting"

                bio_block = profile_soup.find('div', id='accordion-biografia')

#in biograohy block find the commision's section and collect information about commisions senator is participating

                comm_block = profile_soup.find('div', id='comissoes')
                commissions = []
                if comm_block:
                    tbody = comm_block.find('tbody')
                    if tbody:
                        commissions = [
                            comm.find_all('td')[0].get_text(strip=True)
                            for comm in tbody.find_all('tr')
                            if comm.find_all('td')
                        ]

#in biograohy block find the commision's section and collect information about mandates
#MANDATES HERE ARE ALL TERMS IN GOVERNMENT AUTHORITIES (Deputado, Prefeito, Vice-governador, Governador and Senador)

                mandates_terms = bio_block.find('table', class_='table table-striped', title='Mandatos do(a) senador(a)') if bio_block else None
                mandates = []
                if mandates_terms:
                    rows = mandates_terms.find('tbody').find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 3:
                            mandate = {
                                'Position': cols[0].get_text(strip=True),
                                'Start date': cols[1].get_text(strip=True),
                                'End date': cols[2].get_text(strip=True)
                            }
                            mandates.append(mandate)

#IN TERMS_COUNT PUT THE NUMBER OF TERMS SERVED IN SENATE (= terms served as senador in Federal Senate)

                prof_info = profile_soup.find('div', id='accordion-mandatos-exercicios')
                terms_count = prof_info.get_text(strip=True).lower().count('legislaturas') if prof_info else 0

#in biography block find the education part and looking for the degree information

                degree = "N/A"
                educ_table = bio_block.find('table', class_='table table-striped', title='Histórico acadêmico do(a) senador(a)') if bio_block else None
                if educ_table:
                    levels = educ_table.find('tbody').find_all('tr')
                    if levels:
                        h_level = levels[-1].find_all('td')
                        if len(h_level) >= 2:
                            degree = h_level[1].get_text(strip=True)

#as politicians being voted as a part of chapa (group with main candidate, 1st alternate and 2nd alternate) we identify the positio in chapa (=ticket) for every senator

                ticket = "N/A"
                chapa_table = profile_soup.find('table', class_='table table-striped', title='Chapa eleitoral do Senador')
                if chapa_table:
                    tickets = chapa_table.find('tbody').find_all('tr')
                    for idx, ticket_row in enumerate(tickets):
                        ticket_text = ticket_row.get_text(strip=True)
                        if name in ticket_text:
                            if idx == 0:
                                ticket = "Holder"
                            elif idx == 1:
                                ticket = "1st alternate"
                            elif idx == 2:
                                ticket = "2nd alternate"
                            break

#in biography block find the tags for professions

                professions = []
                if bio_block:
                    h3_tags = bio_block.find_all('h3')
                    for h3 in h3_tags:
                        if 'profissões' in h3.get_text(strip=True).lower():
                            ul_tag = h3.find_next_sibling('ul')
                            if ul_tag:
                                li_tags = ul_tag.find_all('li')
                                professions = [li.get_text(strip=True) for li in li_tags]
                            break

#adding all data together

                senators_data.append({
                    "Full Name": f_name,
                    "Short Name": name,
                    "Date of Birth": dob,
                    "Place of Birth": pob,
                    "Sex": sex,
                    "Status": status,
                    #"Office": office,
                    #"Phone": phone,
                    #"E-mail": mail,
                    #"Supoffice": supoffice,
                    "Party": party,
                    "Position in party": position,
                    "Education level": degree,
                    "Professions": professions,
                    "Number of terms in Senate": terms_count,
                    "Position in the last ticket": ticket,
                    "Mandates": mandates,
                    "Commissions": commissions
                })

                print(f" → Processed: {name}")
                time.sleep(0.1)

            except Exception as e:
                print(f"Error processing a senator: {e}")
                continue

print(f"Main scrape complete. Senators processed: {len(senators_data)}")

###2. SCRAPPING INFO ABOUT SENATORS - PARTY and PART - ABBREVIATION FROM THE ALT_URL

for leg in legislatures_alt:
    print(f"Updating party data from legislature {leg}ª...")
    leg_url = f"{alt_url}/{leg}"
    try:
        response_alt = session.get(leg_url)
        if response_alt.status_code != 200:
            print(f"Failed to fetch page for legislatura {leg} (status {response.status_code})")
            continue
        soup_alt = BeautifulSoup(response_alt.text, 'lxml')

#going through all legislatures collect the senator name (short name) and party (abbreviation) he/she is member of

        result = soup_alt.find('table', class_='table', id = 'senadoreslegislaturasanteriores-tabela-senadores')
        if result:
            rows = [
                row for row in result.find('tbody').find_all('tr')
                if row.get('data-suplente') in {'0','1'}
            ]
            for row in rows:
                columns = row.find_all('td')
                name_tag = columns[0].find('a')
                s_name = name_tag.get_text(strip=True) if name_tag else safe_get_text(columns, 0)
                party = safe_get_text(columns, 1)
                if s_name:
                    short_name_party_map[s_name] = party
        else:
            print(f"No senators rable found for legislatura {leg}. Page structure may have changed.")
    except Exception as e:
        print(f"Error processing legislature {leg}: {e}")

#updating information on party affiliation for those senators we found on alternative website

for senator in senators_data:
    if senator["Party"] == "N/A":
        alt_party = short_name_party_map.get(senator["Short Name"])
        if alt_party:
            if alt_party == "-":
                alt_party = "S/Partido"
            senator["Party"] = alt_party

###3. GETTING THE FULL NAME OF THE PARTY AND ABBREVIATION FROM THE ALT URL

for leg in legislatures_alt:
    print(f"Scraping party abbreviation-name pairs for legislature {leg}ª...")
    party_url = f"{alt_url}/{leg}/por-partido"
    try:
        response_p = session.get(party_url)
        soup_p = BeautifulSoup(response_p.text, 'lxml')
        result_p = soup_p.find('table', class_='table', id = 'senadoreslegislaturasanteriores-tabela-senadores')
        if result_p:
            for party in result_p.find('tbody').find_all('tr', class_='search-group-row'):
                split_parts = party.get_text(strip=True).split(' - ', 1)

                if len(split_parts) != 2:
                    continue
                party_abb = split_parts[0].strip()
                party_name = split_parts[1].strip()

                if party_abb not in party_fullname_map:
                    party_fullname_map[party_abb] = party_name

    except Exception as e:
        print(f"Error scrapping party full names: {e}")
    time.sleep(0.1)

###4. MERGE ALL TOGETHER AND EXPORT

long_data = []
for sen in senators_data:
    base_info = {
        "Full Name": sen["Full Name"],
        "Short Name": sen["Short Name"],
        "Date of Birth": sen["Date of Birth"],
        "Place of Birth": sen["Place of Birth"],
        "Sex": sen["Sex"],
        "Status": sen["Status"],
        #"Office": sen["Office"],
        #"Phone": sen["Phone"],
        #"E-mail": sen["E-mail"],
        #"Supoffice": sen["Supoffice"],
        "Party Abbreviation": sen["Party"],
        "Party Full Name": party_fullname_map.get(sen["Party"], "N/A"),
        "Position in party": sen["Position in party"],
        "Education level": sen["Education level"],
        "Number of terms in Senate": sen["Number of terms in Senate"],
        "Position in the last ticket": sen["Position in the last ticket"],
        "Commissions": ", ".join(sen.get("Commissions", [])) if sen.get("Commissions") else "N/A"
    }


    for i, prof in enumerate(sen.get("Professions", []), 1):
        key = "Profession" if i == 1 else f"Profession_{i}"
        base_info[key] = prof

    if sen["Mandates"]:
        for i, mandate in enumerate(sen["Mandates"], 1):
            mandate_row = base_info.copy()
            mandate_row["Mandate number"] = i
            mandate_row["Mandate - Position"] = mandate["Position"]
            mandate_row["Mandate - Start date"] = mandate["Start date"]
            mandate_row["Mandate - End date"] = mandate["End date"]
            long_data.append(mandate_row)
    else:
        no_mandate_row = base_info.copy()
        no_mandate_row["Mandate number"] = "N/A"
        no_mandate_row["Mandate - Position"] = "N/A"
        no_mandate_row["Mandate - Start date"] = "N/A"
        no_mandate_row["Mandate - End date"] = "N/A"
        long_data.append(no_mandate_row)

df = pd.DataFrame(long_data)
df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"Data saved to CSV. Total rows: {len(df)}")
