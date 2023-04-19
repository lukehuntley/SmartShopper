from playwright.sync_api import sync_playwright
import pandas as pd
import requests
import logging
import json
import math
import logging 
import numpy as np
# pd.options.mode.chained_assignment = None

class Extract():

    def __init__(self, product_url:str): 
        self.product_url = product_url      
        
    def _get_cookie(self, url)->str:

        with sync_playwright() as p:
            
            browser = p.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            page.goto(url)
            cookie_for_requests = context.cookies()

            cookie = ''

            for item in cookie_for_requests:
                if item['name'] == '_abck':
                    cookie = item['value']
                    break
            else:
                logging.error('Could not get cookie')

            context.close()
            browser.close()

        return cookie   

    def _create_header(self, url)->dict:

        cookie_string = f"_abck={self._get_cookie(url = url)}"

        header = {
        'content-type': 'application/json',
        'cookie': cookie_string,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }

        return header   
     
    def _get_page_count(self, url:str, header:dict, payload:str)->int:   

        response = requests.request("POST", url=url, headers=header, data=payload)

        if(response.status_code == 200):
            
            json_response = response.json()            
            items_in_category = json_response['TotalRecordCount']    
            pages_in_category = math.ceil(items_in_category / 24)
            
        else: 
            logging.error(response)
        
        return pages_in_category

    
    def _extract_products(self, url:str, header:dict, payload:str)->pd.DataFrame:

        response = requests.request("POST", url=url, headers=header, data=payload)

        if response.status_code== 200:
            json_response = response.json()
            
            with open('json_response.json', 'w', encoding='utf-8') as responseFile:
               responseFile.write(json.dumps(json_response))

            df_products = pd.json_normalize(json_response['Bundles'],'Products')
            
            return df_products
        
        else: 
            logging.error(response)
    
    
    def run(self)->list:

        # df = pd.DataFrame()

        # Prepare header
        header = self._create_header(url=self.product_url)

        # Read list of categorie payloads from file
        with open('../../utility/payload_categories.json', 'r', encoding='utf-8') as payloadFile:
            payloads = json.load(payloadFile)

        category_count = 0

        df_list = []

        for payload in payloads:

            page_df = pd.DataFrame()
            category_df = pd.DataFrame()

            category = payload['url'].split('/')[-1]
            category = category.replace('-','_') 
            category_count += 1
            logging.info(f'Reading category {category_count}:{category}')

            pages_in_category = self._get_page_count(url=self.product_url, header=header, payload=json.dumps(payload))

            for page in range(1, pages_in_category + 1):

                logging.info(f'Reading page: {page} / {pages_in_category} of cateogry {category_count}:{category}')

                # alter payload with page number
                payload.update({'pageNumber': page})
                
                page_df = self._extract_products(url=self.product_url, header=header, payload=json.dumps(payload))                    
                category_df = pd.concat((category_df, page_df), axis = 0)

            # add category column
            category_df = category_df.copy()
            category_df.insert(loc=0, column='Category', value=category)
            
            # append to main df 
            # df = pd.concat((df, category_df), axis = 0)

            # Remove nan
            category_df = category_df.replace({np.nan: None})

            # Truncate column names
            new_column_dict = {}
            for column in category_df.columns:
                new_column_dict[column] = column.replace('.','').replace('_','').replace('Attributes','Attr').replace('Maximum','Max').replace('Minimum','Min').replace('ThirdPartyProductInfoThirdParty','ThirdPartyProduct').replace('Additional','Add').replace('Value','Val').replace('Position','Pos').replace('Option','Opt').replace('Childrens','Child').replace('Size','Sz').replace('Clothing','Cloth').replace('Display','Disp').replace('Colour','Col')

            category_df.rename(columns=new_column_dict, inplace=True)

            # Add category df to list of dataframes
            df_list.append(category_df)

            # df.to_csv('PayloadAll',sep=',' ,encoding='utf-8')

            # logging.info(f'Total dataframe length: {len(df)}') 

        return df_list