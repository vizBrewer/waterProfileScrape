#!/usr/bin/env python
# coding: utf-8

# Runs in a virtual env running these packages, install tabula py,
import requests
from tabula import read_pdf,convert_into
import pandas as pd
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

#iterates over dataframe to handle nulls
def iter_pd(df):
    for val in df.columns:
        yield val
    for row in df.to_numpy():
        for val in row:
            if pd.isna(val):
                yield ""
            else:
                yield val

def pandas_to_sheets(pandas_df, sheet, clear = True):
    # Updates all values in a workbook to match a pandas dataframe
    if clear:
        sheet.clear()
    (row, col) = pandas_df.shape
    cells = sheet.range("A1:{}".format(gspread.utils.rowcol_to_a1(row + 1, col)))
    for cell, val in zip(cells, iter_pd(pandas_df)):
        cell.value = val
    sheet.update_cells(cells)

#url for hosted file
pdf_url = 'hosted file url'
pdf_file = requests.get(pdf_url)
#savespdf
with open('/home/pi/python/waterenv3/profile.pdf', 'wb') as f:
    f.write(pdf_file.content)

#path to the saved pdf
data = read_pdf('/home/pi/python/waterenv3/profile.pdf',pages=[1,2])
df0 = data[0]
df1 = data[1]
df=pd.concat([df0,df1], ignore_index=True)

df.drop(['1MCL','2Units','Unnamed: 0','Unnamed: 1','3Quant Limit'], axis=1, inplace = True)

minerals = ['Calcium','Magnesium','Sodium','Chloride','Sulfate','Alkalinity, Bicarbonate','pH']

df = df.loc[df['Parameter'] .isin(minerals)]
dft = df.T
dft.columns = dft.iloc[0]
cols = [ 'pH', 'Calcium', 'Magnesium', 'Sodium', 'Chloride','Sulfate','Alkalinity, Bicarbonate']
dft = dft[cols]
dft = dft.iloc[1:]
dft.reset_index(level=0, inplace=True)
dft.rename(columns={"Alkalinity, Bicarbonate": "Bicarbonate","index":"Month"}, inplace=True)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

#need to create a service bot account from the google api to push to sheets.
credentials = ServiceAccountCredentials.from_json_keyfile_name('file path for credentials.json file', scope)

gc = gspread.authorize(credentials)
# google sheet Id
wb_id = 'sheet id for google sheet url'
workbook = gc.open_by_key(wb_id)
sheet_name= "whatever you want the sheet name to be"
sheet = workbook.worksheet(sheet_name)


pandas_to_sheets(dft, sheet,clear=True)

