import os
import scrapy
import pandas as pd
import numpy as np
from scrapy.loader import ItemLoader
from equippo.items import EquippoItem


class ImagedownloaderSpider(scrapy.Spider):
    name = 'imagedownloader'
    print("Image Downloader Constructor Called !!!")
    final_df = pd.read_excel('./scrapy_equippo.xlsx')
    all_files = os.listdir('/home/faizan/web_scraping/Cognitia/equippo/images')
    print("Done reading")

    def start_requests(self):
        yield scrapy.Request(url='https://www.google.com/', callback=self.parse_main_page,
                             dont_filter=True)

    def parse_main_page(self, response):
        total_length = len(self.final_df)
        all_columns = list(self.final_df.columns)
        for i in range(0, total_length):
            for k in range(1, 500):
                if 'Image Link ' + str(k) in all_columns:
                    file_name = self.final_df['Image ' + str(k)].iloc[i]
                    image_link = self.final_df['Image Link ' + str(k)].iloc[i]
                    if file_name is not np.nan and file_name != '' and file_name not in self.all_files:
                        loader = ItemLoader(item=EquippoItem())
                        loader.add_value('image_urls', image_link)
                        loader.add_value('file_name', file_name)
                        yield loader.load_item()
                else:
                    break
