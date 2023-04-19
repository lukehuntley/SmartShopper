from playwright.sync_api import sync_playwright
import pandas as pd
import numpy as np
import requests
import logging
import json
import math
import logging 
# pd.options.mode.chained_assignment = None

class Extract():

    def __init__(self, category_url:str, product_url:str): 
        self.category_url = category_url  
        self.product_url = product_url      
        
    def _get_categories(self, url:str):
        """
        Get list of product categories      
        - `url` : the category API URL
        Returns ?
        """
        df_category = pd.DataFrame()
        header = {
            'content-type': 'application/json',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }

        logging.info('about to ping api')

        response = requests.request("GET", url=url, headers=header)
        
        if response.status_code == 200:            
            json_response = response.json()
            df_category = pd.json_normalize(json_response['ListTopLevelPiesCategories'],'Categories')
        else: 
            logging.error(response)
        
        return df_category
    
    def _get_cookie(self, url)->str:
        """
        Gets a cookie from the url to authorise POST requests        
        - `url` : the products API URL        
        Returns the cookie as string
        """

        with sync_playwright() as p:            
            browser = p.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            page.goto(url)
            cookie_for_requests = context.cookies()

            # cookie = ''

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
        """
        Creates header with the cookie added        
        - `url` : the products API URL
        Returns of header as a dictionary
        """

        cookie_string = f"_abck={self._get_cookie(url = url)}"

        header = {
        'content-type': 'application/json',
        'cookie': cookie_string,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }

        return header 

    def _create_payload(self, category_id, url, location, format_object, page_number:int=1)->dict:
        """
        Creates  payload for the POST request
        - `category_id`: the category ID
        - `url`: the category name
        - `location`: the category name
        - `format_object`: the friendly category name
        - `page_number`: the page number of the category
        Returns payload as dictionary
        """
        
        payload = {
            "categoryId": category_id,
            "pageNumber": page_number,
            "pageSize": 24,
            "sortType": "CUPAsc",
            "url": f"/shop/browse/{url}",
            "location": f"/shop/browse/{location}",
            "formatObject": f'{{"name":"{format_object}"}}',
            "isSpecial": None,
            "isBundle": False,
            "isMobile": False,
            "filters": [],
            "token": "",
            "enableGp": False,
            "isHideUnavailableProducts": False
        }

        return payload  
     
    def _get_page_count(self, url:str, header:dict, payload:str)->int:
        """
        Gets the number of pages for a product category        
        - `url` : the API URL
        - `header` : the request header
        - `payload' : the request payload
        Returns the count of of pages as integer
        """  

        response = requests.request("POST", url=url, headers=header, data=payload)

        if(response.status_code == 200):            
            json_response = response.json()            
            items_in_category = json_response['TotalRecordCount']    
            pages_in_category = math.ceil(items_in_category / 24)            
        else: 
            logging.error(response)
        
        return pages_in_category

    def _extract_products(self, url:str, header:dict, payload:str)->pd.DataFrame:
        """
        Get list of products from API request        
        - `url` : the API URL
        - `header` : the request header
        - `payload' : the request payload
        Returns a dataframe of products
        """

        response = requests.request("POST", url=url, headers=header, data=payload)

        if response.status_code == 200:            
            json_response = response.json()
            
            with open('json_response.json', 'w', encoding='utf-8') as responseFile:
               responseFile.write(json.dumps(json_response))

            df_products = pd.json_normalize(json_response['Bundles'],'Products')
        
        else: 
            logging.error(response)

        return df_products
    
    def run(self)->list:
        """
        Run extract
        """
        logging.info(self.category_url)

        category_df = self._get_categories(url=self.category_url)

        # Prepare variables
        category_count = 0        
        df_list = []

        # Prepare header
        header = self._create_header(url=self.product_url)

        for index, row in category_df.iterrows():
            if not (row['UrlFriendlyName'] == 'specials' or row['UrlFriendlyName'] == 'front-of-store'):
                
                page_df = pd.DataFrame()
                category_df = pd.DataFrame()               
                payload = self._create_payload(category_id=row['NodeId'], url=row['UrlFriendlyName'], location=row['UrlFriendlyName'], format_object=row['Description']) 

                 # Get pages in category
                pages_in_category = self._get_page_count(url=self.product_url, header=header, payload=json.dumps(payload))

                category_name = row['UrlFriendlyName'].replace('-',' ').title().replace(' ','')
                category_count += 1
                logging.info(f'Reading category [{category_count}]:{category_name}')

                for page in range(1, pages_in_category + 1):
                    logging.info(f'Reading page: {page} / {pages_in_category} of category [{category_count}]:{category_name}')

                    # Alter payload with page number
                    payload.update({'pageNumber': page})
                    
                    page_df = self._extract_products(url=self.product_url, header=header, payload=json.dumps(payload))                    
                    category_df = pd.concat((category_df, page_df), axis = 0)

                # Add category column
                category_df = category_df.copy()
                category_df.insert(loc=0, column='Category', value=category_name)

                # Replace nan
                category_df = category_df.replace({np.nan: None})

                # Truncate column names
                new_column_dict = {}
                for column in category_df.columns:
                    new_column_dict[column] = column.replace('.','').replace('_','').replace('Attributes','Attr').replace('Maximum','Max').replace('Minimum','Min').replace('ThirdPartyProductInfoThirdParty','ThirdPartyProduct').replace('Additional','Add').replace('Value','Val').replace('Position','Pos').replace('Option','Opt').replace('Childrens','Child').replace('Size','Sz').replace('Clothing','Cloth').replace('Display','Disp').replace('Colour','Col')

                # Rename df with new columns
                category_df.rename(columns=new_column_dict, inplace=True)

                logging.info(f'category_df length: {len(category_df)}')

                # Add category df to list of dataframes
                df_list.append(category_df)

        return df_list