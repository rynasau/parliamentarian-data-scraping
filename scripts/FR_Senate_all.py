import requests
import pandas as pd
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time

###INSTALL xlrd BEFORE RUNNING IF NOT ALREADY INSTALLED
###CHANGE THESE PATHS BEFORE RUNNING THE CODE###

output_path = r'C:\Users\HONOR\Desktop\RA\France\data\FR_senators_all_1999_2024.csv'
filter_csv_path = r'C:\Users\HONOR\Desktop\RA\France\data\senmat.csv'

###Download the .xls file with the main information about former nad current senators

base_url = "https://data.senat.fr/les-senateurs/"

try:
    print("Downloading the senators .xls dataset...")
    response = requests.get(base_url, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'lxml')
    data_tag = soup.find('a', title ='Informations générales sur les sénateurs - Format .xls')

    if not data_tag:
        print("No data set found, the page structure might have changed")

    else:
        data_url = urljoin(base_url, data_tag['href'])
        resp = requests.get(data_url, timeout=15)
        resp.raise_for_status()

        df = pd.read_excel(BytesIO(resp.content), sheet_name=0)
        print("Data successfully loaded into DataFrame.")

#clean and rename the dataframe
        print("Cleaning and renaming columns...")
        df = df.drop(columns=['Courrier électronique'], errors='ignore')
        df = df.rename(columns={
            'Matricule': "Matriculation",
            'Qualité': 'Civil Status',
            'Nom usuel': 'Last Name',
            'Prénom usuel': 'First Name',
            'État': 'Status',
            'Date naissance': 'Date of Birth',
            'Date de décès': 'Date of Death',
            'Groupe politique': 'Political Group',
            "Type d'app au grp politique": 'Type of membership in the political group',
            'Commission permanente': 'Committee',
            'Circonscription': 'Constituency',
            'Fonction au Bureau du Sénat': 'Position in the Senate Office',
            'PCS INSEE': 'Socio-Professional Category',
            'Catégorie professionnelle': 'Professional category',
            'Description de la profession': 'Profession Description'
        })

        df['Date of Birth'] = pd.to_datetime(df['Date of Birth'], errors='coerce').dt.strftime('%d/%m/%Y')
        df['Date of Death'] = pd.to_datetime(df['Date of Death'], errors='coerce').dt.strftime('%d/%m/%Y')


#based on Civil Status define Sex and add Sex column

        df['Sex'] = df['Civil Status'].str.lower().map({
            'mme': 'Female', 'm.': 'Male'
        }).fillna('N/A')

###LOAD FILTERING CSV TO EXTRACT ONLY SENATORS WHO SERVED DURING 1999-2024
#THIS FILTERING CSV IS THE ONE EXTRACTED FROM SQL

        print("Loading filtering CSV with 1999–2024 senators...")
        df2 = pd.read_csv(filter_csv_path)

        for col in ['eludatdeb', 'eludatelu', 'eludatfin']:
            if col in df2.columns:
                df2[col] = pd.to_datetime(df2[col], errors='coerce').dt.strftime('%d/%m/%Y')

        df2 = df2.fillna('N/A')
        df2 = df2.rename(columns={'senmat' : 'Matriculation'})
        print("Loaded filter data.")

#MERGE DATA FRAMES

        print("Merging datasets on Matriculation...")
        df['Matriculation'] = df['Matriculation'].astype(str)
        df2['Matriculation'] = df2['Matriculation'].astype(str)

        merged_df = pd.merge(df2, df, on='Matriculation', how='left')

        merged_df = merged_df.rename(columns={
            'eludatdeb': 'Mandate Start Date',
            'eludatelu': 'Election Date',
            'eludatfin': 'Mandate End Date',
            'etadebmancod': 'Code Starting the Mandate',
            'etafinmancod': 'Code Ending the Mandate'
        })

        bio_columns = [
            'Matriculation', 'Civil Status', 'Last Name', 'First Name', 'Sex', 'Status',
            'Date of Birth', 'Date of Death', 'Political Group',
            'Type of membership in the political group', 'Committee', 'Constituency',
            'Position in the Senate Office', 'Socio-Professional Category',
            'Professional category', 'Profession Description'
        ]

        final_columns = bio_columns + [col for col in merged_df.columns if col not in bio_columns]
        merged_df = merged_df[final_columns]

        print(f"Merged data frame contains {len(merged_df)} rows.")

# Save to CSV
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print("Merged data saved to FR_senators_all_1999_2024.csv")

except Exception as e:
    print(f"An error occurred: {e}")
