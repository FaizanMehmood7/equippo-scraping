import time
import scrapy
import unidecode
import re
import numpy as np
from scrapy.http import Request
from scrapy_selenium import SeleniumRequest
import pandas as pd
from pydispatch import dispatcher
from scrapy import signals
import requests


class SeleniumEquippoSpider(scrapy.Spider):
    name = 'selenium_equippo'
    main_link = 'https://www.equippo.com/en-AT/catalog'
    links_df = pd.read_excel('./scrapy_equippo.xlsx')
    final_df = pd.DataFrame()
    DOC_SAVE_DIR = './inspection_reports/'

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        print("DUMPING")
        self.final_df.to_excel('./equippo.xlsx', index=False)
        print("DUMPED")

    def start_requests(self):
        yield SeleniumRequest(
            url=self.main_link, wait_time=10, screenshot=True, callback=self.parse_main_page)

    def save_pdf(self, response):
        name = response.meta['name']
        self.logger.info('Saving PDF %s', name)
        with open(name, 'wb') as file:
            file.write(response.body)

    def parse_main_page(self, response):
        driver = response.request.meta['driver']
        total_products = len(self.links_df)
        for i in range(0, total_products):
            final_dictionary = dict()
            link = self.links_df['URL'].iloc[i]
            while True:
                try:
                    driver.get(link)
                    driver.implicitly_wait(5)
                    break
                except:
                    print("Loading exception occured: ", i, " ", link)
                    time.sleep(5)
            print("Waiting to load properly")
            time.sleep(10)
            spec_element = driver.find_element_by_xpath("//div[@class='productDetails__body__equipmentDetails']")
            final_dictionary['Specification HTML'] = spec_element.get_attribute('innerHTML')

            equip_col_names = driver.find_elements_by_xpath("//div[@id='react-equipment-details']/div/div/div/div"
                                                            "[@class='productDetails__body__equipmentDetails__listIt"
                                                            "emName']")
            first_span = driver.find_elements_by_xpath("//div[@id='react-equipment-details']/div/div/div/div[@class="
                                                       "'productDetails__body__equipmentDetails__listItemValue']"
                                                       "/span[1]")
            second_span = driver.find_elements_by_xpath("//div[@id='react-equipment-details']/div/div/div/div[@class="
                                                        "'productDetails__body__equipmentDetails__listItemValue']"
                                                        "/span[2]")
            third_span = driver.find_elements_by_xpath("//div[@id='react-equipment-details']/div/div/div/div[@class="
                                                       "'productDetails__body__equipmentDetails__listItemValue']"
                                                       "/span[3]")
            equip_dict = dict()
            if len(first_span) != len(second_span) != len(third_span) != len(equip_col_names):
                print(len(equip_col_names), " ", len(first_span), " ", len(second_span), " ", len(third_span), " ", i,
                      " ", link)
            else:
                for index in range(0, len(equip_col_names)):
                    comb_value = ''
                    value = first_span[index].text
                    if len(value) >= 1:
                        comb_value = value + ','
                    value = second_span[index].text
                    if len(value) >= 1:
                        comb_value = comb_value + value + ','
                    value = third_span[index].text
                    if len(value) >= 1:
                        comb_value = comb_value + value
                    elif len(comb_value) >= 1:
                        if comb_value[-1] == ',':
                            comb_value = comb_value[:-1]
                    name = equip_col_names[index].text
                    equip_dict[name] = comb_value
            other_dict = dict()
            other_cols = driver.find_elements_by_xpath("//div[@class='productDetails__body__equipmentDetails__list"
                                                       "Container']/div/div[@class='productDetails__body__equipment"
                                                       "Details__listItemName']")
            other_values = driver.find_elements_by_xpath("//div[@class='productDetails__body__equipmentDetails__listCon"
                                                         "tainer']/div/div[@class='productDetails__body__equipmentDetai"
                                                         "ls__listItemValue']/span")
            other_idx = -1
            for col_idx, col in enumerate(other_cols):
                col_text = col.text
                if col_text != 'Attached documents':
                    other_idx += 1
                    other_text = other_values[other_idx].text

                    other_dict["Dimensions " + col_text] = other_text
            driver.find_element_by_xpath("//a[@data-behavior='pdf-link-selector']").click()
            print("Inspection Sleep: ", i)
            time.sleep(25)
            while True:
                try:
                    driver.switch_to.window(driver.window_handles[1])
                    break
                except:
                    print("Trying again: ", i)
                    time.sleep(10)
            pre_image = self.links_df['Title'].iloc[i]
            if self.links_df['Serial Number'].iloc[i] is not np.nan:
                pre_image = pre_image + '-' + str(self.links_df['Serial Number'].iloc[i])
            pre_image = unidecode.unidecode(pre_image)
            pre_image = pre_image.replace(' ', '-').replace(',', '-').replace('.', '-').replace('/', '-')
            pre_image = pre_image.replace('?', '-').replace('\\', '-').replace(':', '-').replace('*', '-')
            pre_image = pre_image.replace('<', '-').replace('>', '-').replace('|', '-').replace('\n', '-')
            pre_image = pre_image.replace('(', '-').replace(')', '-').replace('//', '-').replace('/', '-')
            for k in range(0, 50):
                pre_image = pre_image.replace('--', '-')
            if pre_image[0] == '-':
                pre_image = pre_image[1:]
            inspection = {'URL': link, 'Inspection Link S3': driver.current_url, 'Inspection':
                          pre_image + '.pdf'}

            response = requests.get(inspection['Inspection Link S3'])

            # Write content in pdf file
            pdf = open(self.DOC_SAVE_DIR + inspection['Inspection'], 'wb')
            pdf.write(response.content)
            pdf.close()
            final_dictionary = dict(**final_dictionary, **inspection)
            final_dictionary = dict(**final_dictionary, **equip_dict)
            final_dictionary = dict(**final_dictionary, **other_dict)
            all_keys = list(final_dictionary.keys())
            final_dictionary = {k: [final_dictionary[k]] for k in all_keys}
            row = pd.DataFrame.from_dict(final_dictionary)
            self.final_df = pd.concat([self.final_df, row], axis=0, ignore_index=True)
            print("Sleeping")
            driver.close()

            driver.switch_to.window(driver.window_handles[0])
            time.sleep(3)
            if i % 10 == 0:
                file_name = './equippo_' + str(i) + '.xlsx'
                print("Dumping: ", file_name)
                self.final_df.to_excel(file_name, index=False)
