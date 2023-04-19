from datetime import datetime as dt
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
    
    
    def run(self)->pd.DataFrame:

        df = pd.DataFrame()

        # Prepare header
        header = self._create_header(url=self.product_url)

        # Read list of categorie payloads from file
        with open('../../utility/payload_categories.json', 'r', encoding='utf-8') as payloadFile:
            payloads = json.load(payloadFile)

        category_count = 0

        df = pd.read_csv('C:\shoppingscraper\smartshopper\ingestion\src\woolworths\pipeline\PayloadAll1.csv',sep=',', dtype='unicode')        
        
        # df1.to_json('Payload6badcolumnsonly.json')
        # logging.info(f'{df1.dtypes}')

        # df = df[[
        #     'Category',
        #     'TileID',
        #     'Stockcode',
        #     'Barcode',
        #     'GtinFormat',
        #     'CupPrice',
        #     'InstoreCupPrice',
        #     'CupMeasure',
        #     'CupString',
        #     'InstoreCupString',
        #     'HasCupPrice',
        #     'InstoreHasCupPrice',
        #     'Price',
        #     'InstorePrice',
        #     'Name',
        #     'DisplayName',
        #     'UrlFriendlyName',
        #     'Description',
        #     'SmallImageFile',
        #     'MediumImageFile',
        #     'LargeImageFile',
        #     'IsNew','IsHalfPrice','IsOnlineOnly','IsOnSpecial','InstoreIsOnSpecial','IsEdrSpecial','SavingsAmount','InstoreSavingsAmount','WasPrice',
        #     'InstoreWasPrice','QuantityInTrolley','Unit','MinimumQuantity','HasBeenBoughtBefore','IsInTrolley','Source','SupplyLimit','ProductLimit',
        #     'MaxSupplyLimitMessage','IsRanged','IsInStock','PackageSize','IsPmDelivery','IsForCollection','IsForDelivery','IsForExpress','ProductRestrictionMessage',
        #     'ProductWarningMessage','IsCentreTag','HeaderTag','HasHeaderTag','UnitWeightInGrams','SupplyLimitMessage','SmallFormatDescription','FullDescription',
        #     'IsAvailable','InstoreIsAvailable','IsPurchasable','InstoreIsPurchasable','AgeRestricted','DisplayQuantity','RichDescription','IsDeliveryPass',
        #     'HideWasSavedPrice',
        #     'SapCategories','Brand','IsRestrictedByDeliveryMethod','IsFooterEnabled','Diagnostics','IsBundle','IsInFamily','ChildProducts','UrlOverride',
        #     'DetailsImagePaths','Variety','HasProductSubs','IsSponsoredAd','AdID','AdIndex','IsMarketProduct','IsGiftable','Vendor','Untraceable',
        #     'ThirdPartyProductInfo','MarketFeatures','MarketSpecifications','SupplyLimitSource','Tags','IsPersonalisedByPurchaseHistory','IsFromFacetedSearch',
        #     'NextAvailabilityDate','NumberOfSubstitutes','IsPrimaryVariant','VariantGroupId','CentreTag.TagContent','CentreTag.TagLink','CentreTag.FallbackText',
        #     'CentreTag.TagType','CentreTag.MultibuyData','CentreTag.TagContentText','CentreTag.DualImageTagContent','ImageTag.TagContent','ImageTag.TagLink',
        #     'ImageTag.FallbackText','ImageTag.TagType','ImageTag.MultibuyData','ImageTag.TagContentText','ImageTag.DualImageTagContent','FooterTag.TagContent',
        #     'FooterTag.TagLink',
        #     'FooterTag.FallbackText',
        #     'FooterTag.TagType',
        #     'FooterTag.MultibuyData',
        #     'FooterTag.TagContentText',
        #     'FooterTag.DualImageTagContent',
        #     'AdditionalAttributes.boxedcontents',
        #     'AdditionalAttributes.addedvitaminsandminerals',
        #     'AdditionalAttributes.sapdepartmentname',
        #     'AdditionalAttributes.spf',
        #     'AdditionalAttributes.haircolour',
        #     'AdditionalAttributes.lifestyleanddietarystatement',
        #     'AdditionalAttributes.sapcategoryname',
        #     'AdditionalAttributes.skintype',
        #     'AdditionalAttributes.importantinformation',
        #     'AdditionalAttributes.allergystatement',
        #     'AdditionalAttributes.productdepthmm',
        #     'AdditionalAttributes.skincondition',
        #     'AdditionalAttributes.ophthalmologistapproved',
        #     'AdditionalAttributes.healthstarrating',
        #     'AdditionalAttributes.hairtype',
        #     'AdditionalAttributes.fragrance-free',
        #     'AdditionalAttributes.sapsegmentname',
        #     'AdditionalAttributes.suitablefor',
        #     'AdditionalAttributes.PiesProductDepartmentsjson',
        #     'AdditionalAttributes.piessubcategorynamesjson',
        #     'AdditionalAttributes.sapsegmentno',
        #     'AdditionalAttributes.productwidthmm',
        #     'AdditionalAttributes.contains',
        #     'AdditionalAttributes.sapsubcategoryname',
        #     'AdditionalAttributes.dermatologisttested',
        #     'AdditionalAttributes.wool_productpackaging',
        #     'AdditionalAttributes.dermatologicallyapproved',
        #     'AdditionalAttributes.specialsgroupid',
        #     'AdditionalAttributes.productimages',
        #     'AdditionalAttributes.productheightmm',
        #     'AdditionalAttributes.r&r_hidereviews',
        #     'AdditionalAttributes.microwavesafe',
        #     'AdditionalAttributes.paba-free',
        #     'AdditionalAttributes.lifestyleclaim',
        #     'AdditionalAttributes.alcoholfree',
        #     'AdditionalAttributes.tgawarning',
        #     'AdditionalAttributes.activeconstituents',
        #     'AdditionalAttributes.microwaveable',
        #     'AdditionalAttributes.soap-free',
        #     'AdditionalAttributes.countryoforigin',
        #     'AdditionalAttributes.isexcludedfromsubstitution',
        #     'AdditionalAttributes.productimagecount',
        #     'AdditionalAttributes.r&r_loggedinreviews',
        #     'AdditionalAttributes.anti-dandruff',
        #     'AdditionalAttributes.servingsize-total-nip',
        #     'AdditionalAttributes.tgahealthwarninglink',
        #     'AdditionalAttributes.allergenmaybepresent',
        #     'AdditionalAttributes.PiesProductDepartmentNodeId',
        #     'AdditionalAttributes.parabenfree',
        #     'AdditionalAttributes.vendorarticleid',
        #     'AdditionalAttributes.containsgluten',
        #     'AdditionalAttributes.containsnuts',
        #     'AdditionalAttributes.ingredients',
        #     'AdditionalAttributes.colour',
        #     'AdditionalAttributes.manufacturer',
        #     'AdditionalAttributes.sapcategoryno',
        #     'AdditionalAttributes.storageinstructions',
        #     'AdditionalAttributes.tgawarnings',
        #     'AdditionalAttributes.piesdepartmentnamesjson',
        #     'AdditionalAttributes.brand',
        #     'AdditionalAttributes.oilfree',
        #     'AdditionalAttributes.fragrance',
        #     'AdditionalAttributes.antibacterial',
        #     'AdditionalAttributes.non-comedogenic',
        #     'AdditionalAttributes.antiseptic',
        #     'AdditionalAttributes.bpafree',
        #     'AdditionalAttributes.vendorcostprice',
        #     'AdditionalAttributes.description',
        #     'AdditionalAttributes.sweatresistant',
        #     'AdditionalAttributes.sapsubcategoryno',
        #     'AdditionalAttributes.claims',
        #     'AdditionalAttributes.antioxidant',
        #     'AdditionalAttributes.phbalanced',
        #     'AdditionalAttributes.wool_dietaryclaim',
        #     'AdditionalAttributes.ophthalmologisttested',
        #     'AdditionalAttributes.sulfatefree',
        #     'AdditionalAttributes.servingsperpack-total-nip',
        #     'AdditionalAttributes.piescategorynamesjson',
        #     'AdditionalAttributes.nutritionalinformation',
        #     'AdditionalAttributes.ovencook',
        #     'AdditionalAttributes.vegetarian',
        #     'AdditionalAttributes.hypo-allergenic',
        #     'AdditionalAttributes.timer',
        #     'AdditionalAttributes.dermatologistrecommended',
        #     'AdditionalAttributes.sapdepartmentno',
        #     'AdditionalAttributes.allergencontains',
        #     'AdditionalAttributes.waterresistant',
        #     'AdditionalAttributes.friendlydisclaimer',
        #     'AdditionalAttributes.recyclableinformation',
        #     'AdditionalAttributes.usageinstructions',
        #     'AdditionalAttributes.freezable',
        #     'Rating.ReviewCount',
        #     'Rating.RatingCount',
        #     'Rating.RatingSum',
        #     'Rating.OneStarCount',
        #     'Rating.TwoStarCount',
        #     'Rating.ThreeStarCount',
        #     'Rating.FourStarCount',
        #     'Rating.FiveStarCount',
        #     'Rating.Average',
        #     'Rating.OneStarPercentage',
        #     'Rating.TwoStarPercentage',
        #     'Rating.ThreeStarPercentage',
        #     'Rating.FourStarPercentage',
        #     'Rating.FiveStarPercentage',
        #     'HeaderTag.BackgroundColor',
        #     'HeaderTag.BorderColor',
        #     'HeaderTag.TextColor',
        #     'HeaderTag.Content',
        #     'HeaderTag.TagLink',
        #     'HeaderTag.Promotion',
        #     'CentreTag.MultibuyData.Quantity',
        #     'CentreTag.MultibuyData.Price',
        #     'CentreTag.MultibuyData.CupTag',
        #     'AdditionalAttributes.Market.Code',
        #     'AdditionalAttributes.Market.Variant_LegacyId',
        #     'AdditionalAttributes.Market.DescriptionHtml',
        #     'AdditionalAttributes.Market.Gender',
        #     'AdditionalAttributes.Market.UnitWidth',
        #     'AdditionalAttributes.Market.VariantProductIds',
        #     'AdditionalAttributes.Market.IsGiftable',
        #     'AdditionalAttributes.Market.Seller_BusinessName',
        #     'AdditionalAttributes.Market.UnitWeight',
        #     'AdditionalAttributes.Market.Variant_Id',
        #     'AdditionalAttributes.Market.UnitDepth',
        #     'AdditionalAttributes.Market.MaxPurchaseQuantity',
        #     'AdditionalAttributes.Market.Variant_Sku',
        #     'AdditionalAttributes.Market.Seller_Id',
        #     'AdditionalAttributes.Market.AdvertId',
        #     'AdditionalAttributes.Market.IsPrimaryVariant',
        #     'AdditionalAttributes.Market.SpecificationsHtml',
        #     'AdditionalAttributes.Market.IsBulky',
        #     'AdditionalAttributes.Market.UnitHeight',
        #     'ThirdPartyProductInfo.VendorName',
        #     'ThirdPartyProductInfo.ProductMessagePrefix',
        #     'ThirdPartyProductInfo.DeliveryMessagePrefix',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.VendorId',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.VendorName',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.DayRangeDispatchNote',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.HasGiftProducts',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.MinimumDeliveryTime',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.MaximumDeliveryTime',
        #     'ThirdPartyProductInfo.ThirdPartyVendorDeliveryInfo.ShipmentType',
        #     'ThirdPartyProductInfo.DeliveryFlatFee',
        #     'AdditionalAttributes.Market.Variant_Option_Keys',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_289_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionValue_289_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_289_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionValue_289_Size_g_kg_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_289_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3428_Size_g_kg_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3428_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3428_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3428_Size_g_kg',
        #     'AdditionalAttributes.Market.Features',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3428_Size_g_kg',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3537_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3091_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3537_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3537_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3091_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3537_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3091_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3537_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3091_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3091_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3109_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3271_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3109_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3271_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3271_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3271_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3109_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3109_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3271_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3109_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3275_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3275_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3275_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3275_AllSizes',
        #     'AdditionalAttributes.Market.BasePriceEstablished',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3275_AllSizes',
        #     'AdditionalAttributes.tgaextrainformation',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3259_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3259_AllSizes',
        #     'AdditionalAttributes.nutritioninformation',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3259_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3259_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3259_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3600_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3600_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3600_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3600_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3600_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3081_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3081_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3081_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3081_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3081_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3361_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3361_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3361_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3361_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3361_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3273_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3273_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3273_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3273_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3273_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3605_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3605_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3605_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3605_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3605_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_220_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_220_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_220_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_220_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_220_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_215_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_215_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_215_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_215_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_215_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_195_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_913_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValue_913_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValue_195_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_195_Colour',            
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_913_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValue_195_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_913_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_195_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_196_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_196_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_196_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_196_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_196_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_205_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_205_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_205_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_205_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_205_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_218_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_218_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_218_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_218_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_218_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3083_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3083_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3083_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3083_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3363_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3363_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3363_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3363_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3363_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3083_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3086_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3086_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3086_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3086_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3086_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_1939_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_1939_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_1939_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_1939_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_1939_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3277_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3277_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3277_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3277_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3277_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_199_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3417_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_199_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3417_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3417_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3417_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_199_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_199_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3417_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_199_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_222_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_222_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_222_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_222_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_222_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_214_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_214_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_214_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_214_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_214_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_193_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_193_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_193_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_193_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_193_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_206_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_206_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_206_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_206_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_206_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_209_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_209_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_209_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_209_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_209_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_212_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_212_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_212_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_212_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_212_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_204_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_204_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_204_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_204_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_204_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_197_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_197_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_197_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_197_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_197_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_216_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_216_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_216_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_216_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_216_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3262_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3262_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3262_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3262_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3262_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3287_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3287_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3287_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3287_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3287_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_226_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_226_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_226_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_226_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_226_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3281_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3281_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3281_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3281_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3281_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_223_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_223_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_223_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_223_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_223_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_225_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_225_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_225_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_225_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_225_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_1071_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_1071_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_1071_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_1071_Colour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_1071_Colour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3584_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3584_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3584_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3584_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3584_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3293_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3293_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3293_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3293_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3293_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3280_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3280_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3280_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3280_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3280_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_901_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValue_901_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_901_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_901_BottleSize_ml',            
        #     'AdditionalAttributes.Market.Variant_OptionValue_3258_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3258_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3258_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3258_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3258_AllSizes',
        #     'AdditionalAttributes.Market.TGA_UntraceablePrivatePrice',
        #     'AdditionalAttributes.tgauntraceableprivateprice',
        #     'AdditionalAttributes.Market.Variant_OptionValue_911_BottleSize_ml_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_911_BottleSize_ml',            
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_911_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_911_BottleSize_ml',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3084_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3269_AllSizes_Names',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3269_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3269_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3084_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValue_3084_Flavour_Names',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3084_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionValuePosition_3269_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionDisplayName_3084_Flavour',
        #     'AdditionalAttributes.Market.Variant_OptionPosition_3269_AllSizes',
        #     'AdditionalAttributes.Market.Variant_OptionValue_913_BottleSize_ml_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_901_BottleSize_ml_Names',
        #     'AdditionalAttributes.Market.Variant_OptionValue_911_BottleSize_ml'
        # ]]

        df = df.replace({np.nan: None})
        #df['AdditionalAttributes.Market.Variant_OptionValue_913_BottleSize_ml1'] = '1'
        # 'AdditionalAttributes.Market.Variant_OptionValue_913_BottleSize_ml'
        # logging.info(df['AdditionalAttributes.Market.Variant_OptionValue_913_BottleSize_ml'])

        new_column_dict = {}
        for column in df.columns:
            new_column_dict[column] = column.replace('.','').replace('_','').replace('Attributes','Attr').replace('Maximum','Max').replace('Minimum','Min').replace('ThirdPartyProductInfoThirdParty','ThirdPartyProduct').replace('Additional','Add').replace('Value','Val').replace('Position','Pos').replace('Option','Opt').replace('Childrens','Child').replace('Size','Sz').replace('Clothing','Cloth').replace('Display','Disp').replace('Colour','Col')

        df.rename(columns=new_column_dict, inplace=True)

        # logging.info(df.columns)

        # df = df.applymap(str)
        # df=df.astype(str)
        # logging.info(f'{df.dtypes}')
        
        # res = df.dtypes.to_frame('dtypes').reset_index()
        # d = res.set_index('index')['dtypes'].astype(str).to_dict() 

        # with open('types.json', 'w') as f:
        #     json.dump(d, f)

        
        
        # for payload in payloads[4:5]:
        # # for payload in payloads:
        # # for payload in payloads:

        #     page_df = pd.DataFrame()
        #     category_df = pd.DataFrame()

        #     category = payload['url'].split('/')[-1] 
        #     category_count += 1
        #     logging.info(f'Reading category {category_count}:{category}')

        #     pages_in_category = self._get_page_count(url=self.product_url, header=header, payload=json.dumps(payload))

        #     for page in range(1, pages_in_category + 1):
        #     # for page in range(1, 4):  

        #         logging.info(f'Reading page: {page} / {pages_in_category} of cateogry {category_count}:{category}')

        #         # alter payload with page number
        #         payload.update({'pageNumber': page})
                
        #         page_df = self._extract_products(url=self.product_url, header=header, payload=json.dumps(payload))                    
        #         category_df = pd.concat((category_df, page_df), axis = 0)

        #     # add category column
        #     category_df = category_df.copy()
        #     category_df.insert(loc=0, column='Category', value=category)
            
        #     # append to main df 
        #     df = pd.concat((df, category_df), axis = 0)

        #     df.to_csv('Payload7only',sep=',' ,encoding='utf-8')

        #     logging.info(f'Total dataframe length: {len(df)}')

        # with open('json_columns.json', 'w', encoding='utf-8') as responseFile:
        #        responseFile.write(json.dumps(df.columns))

        # logging.info(type(df.columns))
        # logging.info(df.columns)

        columnlist = []
        for column in df.columns:
            columnlist.append(column)

        with open('columns.txt', 'w') as f:
            for line in columnlist:
                f.write(f"{line}\n")

        
        # df.columns.to_csv('json_columns.csv')

        # logging.info('about to return df of len 0')
        return df
        # pass    