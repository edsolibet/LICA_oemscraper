# -*- coding: utf-8 -*-
"""
Created on Thu Oct  6 13:53:13 2022

@author: carlo
"""
import pandas as pd
import numpy as np
import re, os
from datetime import datetime
import datetime as dt
import time
import gspread
import requests, openpyxl
from io import BytesIO
from pytz import timezone

import streamlit as st
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from st_aggrid import GridOptionsBuilder, AgGrid

# to run selenium in headless mode (no user interface/does not open browser)
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument("--disable-gpu")
options.add_argument("--disable-features=NetworkService")
options.add_argument("--window-size=1920x1080")
options.add_argument("--disable-features=VizDisplayCompositor")

# set timezone
phtime = timezone('Asia/Manila')

def clean_oem(x):
    if re.search('^[0-9]*.0', x):
        return x[:-2]
    else:
        return re.sub('(?<=[a-zA-Z0-9]{5})(.*)(?=[a-zA-Z0-9]{5})', '-', x)

def bypass_error(item):
    try:
        return item
    except IndexError:
        pass
    return ''

def get_info(info_type, info):
    '''
    Helper function to scrape info from each product.
    
    Parameters
    ----------
    info_type: string
        Label name of info ('Part Number', 'Description', etc)
    info:
        String to extract info from. Obtained from driver.find_element()
    
    Returns
    -------
    Value of label info
    
    Example:
        get_info('Part Number', info)
        
    
    '''
    if re.search("(?<=" + info_type + ":).*", info):
        return re.search("(?<=" + info_type + ":).*", info)[0].strip()
    else:
        return ''

def write_to_gsheet(df, key):
    '''
    Creates new sheet in designated googlesheet and writes selected data from df
    
    Parameters
    ----------
    df: dataframe
        dataframe to write to google sheet
    
    '''
    credentials = {
      "type": "service_account",
      "project_id": "xenon-point-351408",
      "private_key_id": "f19cf14da43b38064c5d74ba53e2c652dba8cbfd",
      "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5fe2N4yS74jTP\njiyv1EYA+XgnrTkZHwMx4ZY+zLuxx/ODPGxJ3m2e6QRUtz6yBUp1DD3nvzaMYY2d\nea6ti0fO2EPmmNIAZzgWVMOqaGePfXZPN1YN5ncLegZFheZuDrsz0/E+KCVUpLbr\nWBRTBF7l0sZ7paXZsVYOu/QAJI1jPRNF3lFUxMDSE8eGx+/oUmomtl+NfCi/FEJ5\nFCU4pF1FQNmVo885HGe9Tx7UywgaXRvAGJZlA4WVie4d5Jhj8LjZRhSH8+uDgdGX\ngc/4GI8U831oQ2lHsrtYIHHNzs1EG/8Ju+INdgR/Zc5SxNx/BSF8gV7kSueEd8+/\nXlobf5JZAgMBAAECggEAHRPWBOuKCx/jOnQloiyLCsUQplubu0nmxM+Br3eFptFa\n5YQ3z36cPZB2mtcc72gv61hPbgBGC0yRmBGGpfLS/2RchI4JQYHsw2dnQtPaBB7d\nSH66sTQjDjwDNqvOWwtZIj9DroQ5keK+P/dPPFJPlARuE9z8Ojt365hgIBOazGb2\ngIh9wLXrVq7Ki8OXI+/McrxkH3tDksVH2LmzKGtWBA56MRY0v9vnJFjVd+l8Q+05\nIw4lQXt55dK7EmRLIfLnawHYIvnpalCWPe6uAmCTeoOuGASLFJJR2uzcOW9IxM0a\nMkR2dduu5vQl/ahJwxZ2cH40QJUdy7ECQg5QG4qL1wKBgQDugyaPEdoUCGC6MUas\nFR4kwDIkHj/UkgzYtsemmGG0rXCqVtIerPd6FvtKlN8BDzQbyqCaw/pDUqjFoGXN\nW969vkN5Uj9YaQ5qV8c9WLbCcMw9gT6rvqyC8b8FgwaWMKHx7TgI/8xXQ666XqpT\nMTAfINWWei0e/Scqqu6hw0v+UwKBgQDHF5ce9y9mHdVb8B7m0Oz4QIHksktKfoQa\nLoGS601zK6Rr6GeEHb03s4KLG5q9L/o9HUTXqyKERnofdEdfsGsnrKbz2Wsnr8Mk\nGwnNcPTvI3uYkeTBS4paNUxZyGVbxDOrRbBYukgwacaUIGbZ5+we1BxlVN04+l5W\nvAlNEvlfIwKBgBWMcdJhOYOv0hVgWFM5wTRuzNjohrnMzC5ULSuG/uTU+qXZHDi7\nRcyZAPEXDCLLXdjY8LOq2xR0Bl18hVYNY81ewDfYz3JMY4oGDjEjr7dXe4xe/euE\nWY+nCawUz2aIVElINlTRz4Ne0Q1zeg30FrXpQILM3QC8vGolcVPaEiaTAoGBALj7\nNjJTQPsEZSUTKeMT49mVNhsjfcktW9hntYSolEGaHx8TxHqAlzqV04kkkNWPKlZ2\nR2yLWXrFcNqg02AZLraiOE0BigpJyGpXpPf5J9q5gTD0/TKL2XSPaO1SwLpOxiMw\nkPUfv8sbvKIMqQN19XF/axLLkvBJ0DWOaKXwJzs5AoGAbO2BfPYQke9K1UhvX4Y5\nbpj6gMzaz/aeWKoC1KHijEZrY3P58I1Tt1JtZUAR+TtjpIiDY5D2etVLaLeL0K0p\nrti40epyx1RGo76MI01w+rgeZ95rmkUb9BJ3bG5WBrbrvMIHPnU+q6XOqrBij3pF\nWQAQ7pYkm/VubZlsFDMvMuA=\n-----END PRIVATE KEY-----\n",
      "client_email": "googlesheetsarvin@xenon-point-351408.iam.gserviceaccount.com",
      "client_id": "108653350174528163497",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/googlesheetsarvin%40xenon-point-351408.iam.gserviceaccount.com"
    }
    
    gsheet_key = key
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(gsheet_key)
    
    new_sheet_name = datetime.strftime(phtime.localize(datetime.today()),"%B_%d")
    r,c = df.shape
    
    try:
        sh.add_worksheet(title=new_sheet_name,rows = r+1, cols = c+1)
        worksheet = sh.worksheet(new_sheet_name)
    except:
        worksheet = sh.worksheet(new_sheet_name)
        worksheet.clear()
    worksheet.update([df.columns.tolist()]+df.values.tolist())

# PROVIDED DATA
sheet_id = "1t9rMGXKOGrIlkfpURk9Elkcfai1q9SdXYjUF3RY8k80"
sheet_name = 'oem'
url = "https://docs.google.com/spreadsheets/export?exportFormat=xlsx&id=" + sheet_id
res = requests.get(url)
data = BytesIO(res.content)
xlsx = openpyxl.load_workbook(filename=data)
data = pd.read_excel(data, sheet_name = sheet_name)

# CLEANING
data.loc[:, 'OEM no'] = data.apply(lambda x: clean_oem(str(x['OEM no'])), 1)


# OEMPARTSONLINE
driver = Chrome(options=options)
df_parts_ = []
full_brands = ['TOYOTA', 'HYUNDAI', 'MITSUBISHI', 'NISSAN', 'HONDA', 'ISUZU',
               'CHEVROLET', 'FORD', 'KIA', 'MAZDA', 'SUZUKI', 'DAEWOO', 'FOTON', 'MG']
full_part_cats = ['air-filters', 'air-intake', 'alternators', 'belts', 'brake-pads',
                  'brakes', 'control-arms', 'drive-shaft', 'engine-parts', 
                  'fuel-system', 'headlights', 'ignition', 'lighting', 
                  'oil-filters', 'radiators', 'spark-plugs', 'starters', 'weather-stripping',
                  'wiper-blades']
part_cats = ['air-filters', 'oil-filters', 'fuel-system']
for brand in full_brands:
    try:
        print (f"Getting info from {brand}")
        base_url = "https://" + brand.lower() + ".oempartsonline.com/"
        driver.get(base_url)
    except:
        print (f"{brand.lower()} not available.")
        continue
    
    for cat in full_part_cats:
        try:
            driver.get(base_url + cat)
        except:
            print (f"{cat} not available.")
            continue
        print (f"Getting {brand}-{cat}")
        parts_list = []
        parts_list_dict = {}
        parts = driver.find_elements(By.XPATH, '//div[@class="col-xs-12 col-md-8"]')
        for index, part in enumerate(parts):
            product_info = part.text.split('\n')
            parts_list_dict[index] = {'Name': product_info[0].strip()}
            for info in product_info:
                # brand
                parts_list_dict[index]['Brand'] = brand
                for label in ['Part Number', 'Other Names', 'Description', 'Notes', 'Fits', 'Replaces']:
                    if re.search(label, info):
                        parts_list_dict[index][label] = get_info(label, info)
                    else:
                        continue
   
        df_parts_.append(pd.DataFrame.from_dict(parts_list_dict, orient="index"))
    
df_data = pd.concat(df_parts_, axis=0)

# VIC Parts
def import_data():
    # https://docs.google.com/spreadsheets/d/1IvNcA3QUeDjRY2IyHJvNmzewWmSVt6kLm6QKst_3JoU/edit#gid=2071583396
    sheet_id = "1IvNcA3QUeDjRY2IyHJvNmzewWmSVt6kLm6QKst_3JoU"
    sheet_name = 'Products_Info'
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
    return pd.read_csv(url)

# GET VIC PRODUCTS
df_products_info = import_data()
vic_products = df_products_info[df_products_info['Brand Name'] == 'VIC']
vic_prod_numbers = sorted(list(vic_products['Product Name'].apply(lambda x: re.search('(?<=FILTER )[A-Z][A-Z]?-?[0-9]{3}[A-Z]?', x.upper())[0] 
                                                   if re.search('(?<=FILTER )[A-Z][A-Z]?-?[0-9]{3}[A-Z]?', x.upper()) else '').unique()))
vic_prod_numbers.remove('')
vic_prod_numbers = [''.join(n.split('-')) for n in vic_prod_numbers]

vic_number_correction = {'C409' : 'C409A'}

az_homepage = 'https://azfilter.jp/catalogue/catalogue'
part_num_list, car_models = {}, {}
for num in vic_prod_numbers:
    print (f"Collecting info from {num}")
    # SEARCH VIC PRODUCT on AZUMI CATALOGUE
    driver.get(az_homepage)
    driver.implicitly_wait(np.random.randint(2, high=5))
    num = vic_number_correction[num] if num in  vic_number_correction.keys() else num
    driver.find_element(By.ID, 'txtPartNumber')\
        .send_keys(num)
    driver.implicitly_wait(np.random.randint(5, high=8))
    driver.find_elements(By.XPATH, '//button[@type="submit" and @class="btn btn-default btn-primary"]')[0].click()
    driver.implicitly_wait(np.random.randint(2, high=5))
    
    # CHECK IF INTERMEDIARY PAGE SHOWS (TABLE SHOWING VIC OR UNION BRANDS)
    try:
        driver.find_element(By.PARTIAL_LINK_TEXT, num).click()
    except:
        pass
    
    # GET CROSSES
    table = driver.find_elements(By.XPATH, '//table[@class="table table-bordered table-striped marginbtnless"]')
    temp_crosses_table = []
    # SUPPOSEDLY REMOVE SPECS
    # SOME EDGE CASES WITH EXTRA 
    for row in table[1:]:
        temp_crosses_table.extend(row.text.split('\n')[1:])
    print (temp_crosses_table)
    part_num_list[num] = temp_crosses_table
    
    car_models_list = driver.find_elements(By.XPATH, '//div[@class="panel panel-default margintpless"]')
    temp_list = []
    for model in car_models_list:
        temp = model.text.split('»')
        temp_list.append(temp[0].strip() + '_' + temp[1].strip())
    car_models[num] = temp_list

# GET VIC PRODUCTS COMPATIBLE PART NUMBERS
df_vic = pd.concat({k: pd.Series(v, dtype=object) for k, v in part_num_list.items()})\
            .reset_index()\
            .drop(columns = 'level_1')\
            .rename(columns={'level_0': 'part_number', 
                             0: 'compatible_parts'})
df_vic.loc[:, 'brand'] = df_vic.apply(lambda x: x['compatible_parts'].split(' ')[0], 1)
df_vic.loc[:, 'compatible_part_numbers'] = df_vic.apply(lambda x: ''.join(x['compatible_parts'].split(' ')[1:]), 1)
df_vic.drop(columns='compatible_parts', inplace=True)
df_vic = df_vic[df_vic.brand.isin(full_brands)]
# SAVE RESULTS TO CSV
df_vic.to_csv("vic_compatible_part_numbers.csv")

# GET VIC PRODUCTS COMPATIBLE CAR MODELS
df_car_compatible = pd.concat({k: pd.Series(v, dtype=object) for k, v in car_models.items()})\
                        .reset_index() \
                        .drop(columns = 'level_1')\
                        .rename(columns={'level_0': 'part_number',
                                         0: 'compatible_models'})
df_car_compatible.loc[:, 'brand'] = df_car_compatible.apply(lambda x: x['compatible_models'].split('_')[0], 1)
df_car_compatible.loc[:, 'model'] = df_car_compatible.apply(lambda x: x['compatible_models'].split('_')[1], 1) 
df_car_compatible.drop(columns='compatible_models', inplace=True)
df_car_compatible = df_car_compatible[df_car_compatible.brand.isin(full_brands)]
# SAVE RESULTS TO CSV
df_car_compatible.to_csv("vic_compatible_car_models.csv")


