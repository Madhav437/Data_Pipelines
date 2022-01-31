import requests
import time
import wget
from bs4 import BeautifulSoup
import os
import zipfile
import pandas as pd
import calendar

strs = os.getcwd() + '/data'
try:
    for f in os.listdir(strs):
        os.remove(os.path.join(strs, f))
except OSError as e:
    print("")

url = 'https://www.ercot.com/misapp/GetReports.do?reportTypeId=13061&reportTitle=Historical%20RTM%20Load%20Zone%20and%20Hub%20Prices&showHTMLView=&mimicKey'

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

for one_a_tag in soup.findAll('a'):
    link = one_a_tag['href']
    url = 'https://www.ercot.com' + link

    wget.download(url, out=strs, bar=None)

    time.sleep(1)
print("Downloaded files")

# Evaluate Individual Dataframes

strs = os.getcwd() + '/data'
df_final = pd.DataFrame()
try:
    for f in os.listdir(strs):
        if f.find('.zip') == -1:  # Only open Zip files
            continue

        archive = zipfile.ZipFile(strs + '/' + f, 'r')
        file_name = archive.namelist()[0]
        # Read all sheets in the Excel
        di = pd.read_excel(archive.open(file_name), sheet_name=None,
                           usecols="A,E,G")
        df_norm = pd.concat(di, ignore_index=True)

        for i in range(1, 13):

            df = df_norm.loc[(df_norm['Settlement Point Name'] == 'HB_WEST')
                             & (df_norm['Delivery Date'].str.slice(start=0, stop=2) == str(i).zfill(2))]

            if df.empty:
                continue

            month = df['Delivery Date'].iloc[0][0:2]
            month_name = calendar.month_abbr[int(month)]
            year = df['Delivery Date'].iloc[0][-4:]

            if round(df["Settlement Point Price"].mean(), 2) > 100:
                df2 = {'Month': month_name, 'Year': year, 'Settlement': 'HB_WEST',
                       '$/MWh - Avg': round(df["Settlement Point Price"].mean(), 2)}
                df_final = df_final.append(df2, ignore_index=True)

            print(month_name + " " + year + " Processed")

    df_final.to_csv('Higher_Avg.csv', index=False)
except OSError as e:
    print(e)
