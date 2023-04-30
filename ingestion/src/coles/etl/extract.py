import pandas as pd
import numpy as np
import math
import requests
import logging

class Extract():

    def __init__(self, category_url:str, product_url:str, subscription_key:str): 
        self.category_url = category_url  
        self.product_url = product_url
        self.subscription_key = subscription_key      
        
    def _get_categories(self, url:str, headers:dict)->pd.DataFrame:
        """
        Get list of product categories      
        - `url`: the category API URL
        - `headers`: the request headers

        Returns dataframe of categories
        """
        
        category_df = pd.DataFrame()

        response = requests.request("GET", url=url, headers=headers)
        
        if response.status_code == 200:            
            json_response = response.json()
            category_df = pd.json_normalize(json_response['catalogGroupView'])

        else: 
            logging.error('response')
        
        return category_df
    
    def _create_headers(self, headers_for:str, subscription_key:str=None)->dict:
        """
        Creates headers with the cookie added        
        - `headers_for`: the API endpoint the headers is for
        - `subscription_key`: the authentication key for the headers 

        Returns of headers as a dictionary
        """
        
        headers = {}

        if headers_for == 'category':
            headers = {
                'authority': 'www.coles.com.au',
                'accept': '*/*',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json',
                'cookie': 'at_check=true; sessionId=682f1981-3dd2-4784-b49c-42503d7b47d8; visitorId=46ecf776-bf67-403e-9091-43a338a629c2; ai_user=e4V6g8Ejz/HUpD40vlTrjN|2023-04-18T14:35:49.330Z; ApplicationGatewayAffinityCORS=427a90237642a999800eafa64ccf3ab1; ApplicationGatewayAffinity=427a90237642a999800eafa64ccf3ab1; analyticsIsLoggedIn=false; AMCVS_0B3D037254C7DE490A4C98A6%40AdobeOrg=1; BVBRANDID=e4132d2f-4310-492b-a305-66a88f0931d0; dsch-visitorid=c3bed9aa-7803-4f99-b319-34bc317568f3; AMCV_0B3D037254C7DE490A4C98A6%40AdobeOrg=179643557%7CMCIDTS%7C19466%7CMCMID%7C15694532825291125679098083890074771391%7CMCAID%7CNONE%7CMCOPTOUT-1681917662s%7CNONE%7CvVersion%7C5.5.0; fulfillmentStoreId=4824; dsch-sessionid=3c5a3cdf-badf-49fc-84d0-b81a3a8bbd69; ai_session=FVApTyAhGJSMiUndidxwEE|1681904539559|1681912220354; mbox=PC#99215c5e00c34c98bacfaa1bf7e23ce9.36_0#1745155703|session#94a99941c38b4d849b95a41a48f77c25#1681914095; dsch-visitorid=4e6e1616-a718-4ab4-ba66-52776ab23bad',
                'cusp-correlation-id': '7c573df3-af42-4562-987b-2fd353d6aba3',
                'cusp-session-id': '682f1981-3dd2-4784-b49c-42503d7b47d8',
                'cusp-user-id': '',
                'cusp-visitor-id': '46ecf776-bf67-403e-9091-43a338a629c2',
                'dsch-channel': 'coles.online.1site.desktop',
                'ocp-apim-subscription-key': subscription_key,
                'referer': 'https://www.coles.com.au/',
                'sec-ch-ua': '"Chromium";v="112", "Brave";v="112", "Not:A-Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'sec-gpc': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            }

        elif headers_for == 'product':
            headers = {                
                'content-type': 'application/json',               
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            }

        return headers
     
    def _get_products(self, url:str, headers:dict)->pd.DataFrame:
        """
        Get list of products from API request
        - `category`: the category        
        - `url`: the API URL
        - `headers`: the request headers

        Returns a dataframe of products
        """

        products_df = pd.DataFrame()

        response = requests.request("GET", url=url, headers=headers)

        if response.status_code == 200:            
            json_response = response.json()
            product_df = pd.json_normalize(json_response['pageProps']['searchResults'], 'results')
            # Remove items that are not products
            # product_df = product_df[product_df['_type'] != 'SINGLE_TILE']
            product_df = product_df[product_df['_type'] == 'PRODUCT']
        
        else: 
            logging.error(response)

        return products_df
    
    def _get_page_count(self, url:str, headers:dict)->int:
        """
        Gets the number of pages for a product category        
        - `url`: the API URL
        - `headers`: the request headers

        Returns the count of of pages as integer
        """  

        pages_in_category = 0

        response = requests.request("GET", url=url, headers=headers)

        if(response.status_code == 200):            
            json_response = response.json()['pageProps']['searchResults']  
            products_in_category = json_response['noOfResults'] 
            pages_in_category = math.ceil(products_in_category / 48)   

        else: 
            logging.error(response)
        
        return pages_in_category
    
    def run(self)->list:
        """
        Run extract
        """

        category_headers = self._create_headers(headers_for='category', subscription_key=self.subscription_key)
        category_df = self._get_categories(url=self.category_url, headers=category_headers)

        # Prepare variables
        category_count = 0        
        list_of_product_df = []
        
        for index, row in category_df.iterrows():

            product_df = pd.DataFrame()

            if not (row['seoToken'] == 'dropped-locked' or row['seoToken'] == 'back-to-school'):
                
                product_url = f"{self.product_url}{row['seoToken']}.json?slug={row['seoToken']}" 
                category_count += 1
                category_name = row['seoToken'].replace('-',' ').title().replace(' ','')

                logging.info(f'Extracting products for category [{category_count}:{category_name}]')

                # Get pages in category
                product_headers = self._create_headers(headers_for='product')
                pages_in_category = self._get_page_count(url=product_url, headers=product_headers)                   

                for page in range(1, pages_in_category + 1):
                    logging.info(f'Extracting page [{page} / {pages_in_category}] of category [{category_count}:{category_name}]')
                    
                    # Alter URL with page number               
                    product_url = f"{self.product_url}{row['seoToken']}.json?page={page}&slug={row['seoToken']}"    
                    product_headers = self._create_headers(headers_for='product')

                    page_df = self._get_products(url=product_url, headers=product_headers)                      
                    product_df = pd.concat((product_df, page_df), axis = 0) 

                # Add product df to list of dataframes
                if not product_df.empty:
                    # Replace nan
                    product_df = product_df.replace({np.nan: None})                    
                    
                    # Name the df
                    product_df.attrs['name'] = category_name
                                     
                    list_of_product_df.append(product_df)

                else:
                    logging.info(f'{category_name} df is empty')

        return list_of_product_df