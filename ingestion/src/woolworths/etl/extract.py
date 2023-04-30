from playwright.sync_api import sync_playwright
import pandas as pd
import numpy as np
import requests
import logging
import json
import math
import logging 

class Extract():

    def __init__(self, category_url:str, product_url:str): 
        self.category_url = category_url  
        self.product_url = product_url      
        
    def _get_categories(self, url:str, headers:dict)->pd.DataFrame:
        """
        Get list of product categories      
        - `url`: the category API URL
        - `headers`: the request headers

        Returns Dataframe
        """
        
        category_df = pd.DataFrame()

        response = requests.request("GET", url=url, headers=headers)
        
        if response.status_code == 200:            
            json_response = response.json()
            category_df = pd.json_normalize(json_response['ListTopLevelPiesCategories'],'Categories')
        
        else: 
            logging.error(response)
        
        return category_df
    
    def _get_cookie(self, url:str)->str:
        """
        Gets a cookie from the url to authorise POST requests        
        - `url`: the products API URL      

        Returns the cookie as string
        """

        with sync_playwright() as p:            
            browser = p.chromium.launch()
            context = browser.new_context()
            page = context.new_page()
            page.goto(url)
            cookies = context.cookies()

            cookie = ''

            for cookie in cookies:
                if cookie['name'] == '_abck':
                    cookie = cookie['value']
                    break
            else:
                logging.error('Could not get cookie')

            context.close()
            browser.close()

        return cookie   

    def _create_headers(self, headers_for:str, url:str=None)->dict:
        """
        Creates headers with the cookie added        
        - `url`: the products API URL

        Returns of headers as a dictionary
        """

        if headers_for == 'category':
            headers = {
                'content-type': 'application/json',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
                }
            
        elif headers_for == 'product':
            
            cookie_string = f"_abck={self._get_cookie(url=url)}"

            headers = {
            'content-type': 'application/json',
            'cookie': cookie_string,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }

        return headers 

    def _create_payload(self, category_id:str, url:str, location:str, format_object:str, page_number:int=1)->dict:
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
     
    def _get_page_count(self, url:str, headers:dict, payload:str)->int:
        """
        Gets the number of pages for a product category        
        - `url`: the API URL
        - `headers`: the request headers
        - `payload': the request payload

        Returns the count of of pages as integer
        """  

        response = requests.request("POST", url=url, headers=headers, data=payload)

        if(response.status_code == 200):            
            json_response = response.json()            
            products_in_category = json_response['TotalRecordCount']    
            pages_in_category = math.ceil(products_in_category / 24)   

        else: 
            logging.error(response)
        
        return pages_in_category

    def _get_products(self, url:str, headers:dict, payload:str)->pd.DataFrame:
        """
        Get list of products from API request        
        - `url`: the API URL
        - `headers`: the request headers
        - `payload': the request payload

        Returns a dataframe of products
        """

        products_df = pd.DataFrame()

        response = requests.request("POST", url=url, headers=headers, data=payload)

        if response.status_code == 200:            
            json_response = response.json()
            products_df = pd.json_normalize(json_response['Bundles'],'Products')
        
        else: 
            logging.error(response)

        return products_df
    
    def run(self)->list:
        """
        Run extract
        """

        category_headers = self._create_headers(headers_for='category')
        category_df = self._get_categories(url=self.category_url, headers=category_headers)

        # Prepare variables
        category_count = 0        
        list_of_product_df = []

        for index, row in category_df.iterrows():
            
            product_df = pd.DataFrame()

            if not (row['UrlFriendlyName'] == 'specials' or row['UrlFriendlyName'] == 'front-of-store' or row['UrlFriendlyName'] == 'mother-s-day'):
            # if row['UrlFriendlyName'] == 'household':
                category_count += 1
                category_name = row['UrlFriendlyName'].replace('-',' ').title().replace(' ','')
                
                logging.info(f'Extracting products for category [{category_count}:{category_name}]')

                # Get pages in category
                product_headers = self._create_headers(headers_for='product', url=self.product_url)
                product_payload = self._create_payload(category_id=row['NodeId'], url=row['UrlFriendlyName'], location=row['UrlFriendlyName'], format_object=row['Description']) 
                pages_in_category = self._get_page_count(url=self.product_url, headers=product_headers, payload=json.dumps(product_payload))

                for page in range(1, pages_in_category + 1):
                    logging.info(f'Extracting page [{page} / {pages_in_category}] of category [{category_count}:{category_name}]')
                    
                    # Alter payload with page number
                    product_payload.update({'pageNumber': page})
                    
                    page_df = self._get_products(url=self.product_url, headers=product_headers, payload=json.dumps(product_payload))                    
                    product_df = pd.concat((product_df, page_df), axis = 0)

                 # Add product df to list of dataframes
                if not product_df.empty:
                    # Replace nan
                    product_df = product_df.replace({np.nan: None})                    
                    
                    # Truncate column names
                    new_column_dict = {}
                    for column in product_df.columns:
                        new_column_dict[column] = column.replace('.','').replace('_','').replace('Attributes','Attr').replace('Maximum','Max').replace('Minimum','Min').replace('ThirdPartyProductInfoThirdParty','ThirdPartyProduct').replace('Additional','Add').replace('Value','Val').replace('Position','Pos').replace('Option','Opt').replace('Childrens','Child').replace('Size','Sz').replace('Clothing','Cloth').replace('Display','Disp').replace('Colour','Col')

                    # Rename df with new columns
                    product_df.rename(columns=new_column_dict, inplace=True)
                    
                    # Name the df
                    product_df.attrs['name'] = category_name
                  
                    list_of_product_df.append(product_df)

                else:
                    logging.info(f'{category_name} df is empty')
                
        return list_of_product_df