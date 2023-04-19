import requests
import json
from datetime import datetime
import time
import sys
import os
import math
# import asyncio
from playwright.sync_api import sync_playwright

product_url = "https://www.woolworths.com.au/apis/ui/browse/category"

def printWithTime(text):
    now = datetime.now()
    now = '[' + now.strftime('%d-%m-%Y %H:%M:%S') + ']'
    print(f'{now} {text}')

# def generateProductId(item):
#     stockcode = str(item['Stockcode']).replace(' ', '')
#     barcode = str(item['Barcode']).replace(' ', '')
#     return f'w{stockcode}-{barcode}'

# def generateProductDetails(item, category, productId):
#     if not isinstance(item['Barcode'], list):
#         barcode = [item['Barcode']]
#     if item['InstoreHasCupPrice']:
#         cupPrice = item['InstoreCupString'].lower()
#     else:
#         cupPrice = None

#     return {
#         'id': productId,
#         'store': 'woolworths',
#         'name': item['Name'],
#         'brand': toCapitalized(item['Brand']),
#         'price': item['InstorePrice'],
#         'orgPrice': item['InstoreWasPrice'],
#         'categoryIds': ['household' if category == 'lunch-box' else category], # lunch-box will be merged with household
#         'imagePath': item['DetailsImagePaths'][0],
#         'cupPrice': cupPrice,
#         'unit': item['Unit'].lower(),
#         'packageSize': item['PackageSize'].lower(),
#         'barcode': barcode,
#         'isAvailable': True,
#         'locations': ['vic','nsw','qld','wa','sa','tas','nt','act']
#     }

# def toCapitalized(text):
#     if text is None:
#         return ''
#     words = text.split()
#     result = []
#     for word in words:
#         result.append(word.replace(' ', '').capitalize())
#     return ' '.join(result)

def get_cookie_playwright():

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context()
        page = context.new_page()
        page.goto(product_url)
        cookie_for_requests = context.cookies()

        cookie = ''

        for item in cookie_for_requests:
            if item['name'] == '_abck':
                cookie = item['value']
                break
        else:
            print('Did not find item')

        context.close()
        browser.close()

        return cookie   

def create_header():

    cookie_string = f"_abck={get_cookie_playwright()}"

    header = {
    'content-type': 'application/json',
    'cookie': cookie_string,
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
    }

    return header

if __name__ == '__main__':

    start_time = time.time()
    
    printWithTime('Scrapping data from Woolworths')

    # Prepare header
    header = create_header()

    # Read list of categorie payloads from file
    with open('utility/payload_categories.json', 'r', encoding='utf-8') as payloadFile:
        payloads = json.load(payloadFile)

    printWithTime(f'There are {len(payloads)} categories')

    # Get products
    products = []
    category_count = 0
    for payload in payloads[:1]:

        category_count += 1

        category = payload['url'].split('/')[-1]     

        # Determine number of pages
        printWithTime(f'Getting number of items and pages in category {category}')    
        response = requests.request("POST", url=product_url, headers=header, data=json.dumps(payload))
        printWithTime(f'Response: {response.status_code}')

        if(response.status_code == 200):
            
            json_response = response.json()
            
            items_in_category = json_response['TotalRecordCount']
            printWithTime(f'Total items in category {items_in_category}')
    
            pages_in_category = math.ceil(items_in_category / 24)
            printWithTime(f'Total pages in category {items_in_category}')
        else:
            printWithTime('Unable to get page count for category')
            continue

        printWithTime(f"Getting products for category {category_count}: {category}")
        
        for page in range(1, pages_in_category + 1):        

            printWithTime(f"Getting page {page} / {pages_in_category} for category {category_count}: {category}")

            # alter payload with page number
            payload.update({'pageNumber': page})
    
            # run request
            response = requests.request("POST", url=product_url, headers=header, data=json.dumps(payload))
            
            printWithTime(f'Response: {response.status_code}')

            if(response.status_code == 200):
                
                # get only products
                json_products = response.json()['Bundles']
                print(json_products)

                for product in json_products:
                    products.append(product)
                    printWithTime('Appending items to products')
            
            else:
                printWithTime('Unable to get items from category')
                continue

        # Write to product file    
        with open(f"products1_{payload['url'].split('/')[-1]}.json", 'w', encoding='utf-8') as productFile:
            json.dump(products, productFile)
            products.clear()

    # runtime 
    total_runtime = time.time() - start_time
    printWithTime(f'Total runtime: {total_runtime} seconds')
