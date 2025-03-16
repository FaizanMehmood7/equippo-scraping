import re
import scrapy
import unicodedata
import unidecode
from pydispatch import dispatcher
from scrapy import signals
import pandas as pd
from scrapy.loader import ItemLoader


def get_string_from_xpath(response, xpath):
    title = response.xpath(xpath).extract_first()
    if title is not None:
        return re.sub(' +', ' ', (str.strip(str.rstrip(title)).replace('\n', ' ')))
    return ''


class ScrapyEquippoSpider(scrapy.Spider):
    name = 'scrapy_equippo'
    start_urls = ['https://www.equippo.com/en-AT/catalog']
    pre_link = 'https://www.equippo.com'
    final_df = pd.DataFrame()

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        self.final_df.to_excel('./scrapy_equippo.xlsx', index=False)

    def parse(self, response):
        all_pages = response.xpath("//div[@class='paginationSection']/a/@href").extract()
        for page in all_pages:
            page_link = self.pre_link + page
            yield scrapy.Request(url=page_link, callback=self.parse_page,
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                        '(KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
                                          }, dont_filter=True, meta={'url': page_link})

    def parse_page(self, response):
        prod_elements = response.xpath("//div[@class='products__section__cardSpecs']")
        for prod_element in prod_elements:
            prod_category = get_string_from_xpath(prod_element, "./div/div[@class='products"
                                                                "__section__cardSpecsHeader']/a/text()")
            prod_title = get_string_from_xpath(prod_element, "./div/a[@class='products__section__cardSpecsTitle']"
                                                             "/text()")
            prod_price = get_string_from_xpath(prod_element, "./div[@class='products__section__cardSpecs"
                                                             "PriceInfoContainer']/div[@class='products__section__cardS"
                                                             "pecsPriceInfo']/div[@class='products__section__"
                                                             "cardSpecsPrice']/text()")
            prod_link = prod_element.xpath("./div/a[@class='products__section__cardSpecsTitle']/@href").extract_first()
            if prod_link is not None:
                prod_link = self.pre_link + prod_link

            prod_meta_raw = prod_element.xpath("./div/div[@class='products__section__cardSpecsHeader']/"
                                               "div[@class='products__section__cardSpecsSubTitle']/text()").extract()
            prod_meta = ''
            for index, raw_meta in enumerate(prod_meta_raw):
                meta = re.sub(' +', ' ', (str.strip(str.rstrip(raw_meta)).replace('\n', ' ')))
                if len(meta):
                    prod_meta += meta
                    if index < len(prod_meta_raw) - 1:
                        prod_meta += ' | '
            pre_dictionary = {'Category': prod_category, 'Title': prod_title, 'Price': prod_price,
                              'URL': prod_link, 'Meta': prod_meta}
            yield scrapy.Request(url=prod_link, callback=self.parse_product,
                                 headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                                        '(KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36'
                                          }, dont_filter=True, meta={'pre_dictionary': pre_dictionary})

    def parse_product(self, response):
        final_dictionary = dict()
        pre_dictionary = response.meta['pre_dictionary']
        final_dictionary = dict(**final_dictionary, **pre_dictionary)
        img_links = response.xpath("//div[@data-gallery-type='carousel']/img/@data-src").extract()
        img_dict = dict()
        sub_title = response.xpath("//span[@class='productDetails__subTitle']/text()").extract_first()
        if sub_title is not None:
            img_dict['Sub Title'] = re.sub(' +', ' ', (str.strip(str.rstrip(sub_title)).replace('\n', ' ')))
        id_ = response.xpath("//div[@class='productDetails__productIdSellerContainer']/p/text()").extract_first()
        if id_ is not None:
            img_dict['ID'] = re.sub(' +', ' ', (str.strip(str.rstrip(id_)).replace('\n', ' ')))
        details_elements = response.xpath("//div[@class='productDetails__body__headerSpecs__contentItem']")
        for index, details_element in enumerate(details_elements):
            span_ = details_element.xpath(".//span/text()").extract()
            if len(span_) == 2:
                img_dict[span_[0]] = re.sub(' +', ' ', (str.strip(str.rstrip(span_[1])).replace('\n', ' ')))
            else:
                span_value = ''
                for i in range(1, len(span_)):
                    span_value += re.sub(' +', ' ', (str.strip(str.rstrip(span_[i])).replace('\n', ' ')))
                    if i < len(span_) - 1:
                        span_value += ' | '
                img_dict[span_[0]] = span_value
                print("Problem Details: ", span_, span_value)
        condition_elements = response.xpath("//li[@class='inspectionSummary__overallItem']")
        overall_conditions = ''
        for index, condition_element in enumerate(condition_elements):
            span_ = condition_element.xpath(".//span/text()").extract()
            if len(span_) == 2:
                overall_conditions += span_[0] + ' | '
                overall_conditions += span_[1]
                if index < len(condition_elements) - 1:
                    overall_conditions += ' | '
            else:
                print("Problem conditions: ", span_)
        img_dict['Overall condition'] = overall_conditions
        ins = response.xpath("//p[@class='inspectionSummary__commentContent']/text()").extract_first()
        if ins is not None:
            img_dict["Inspector's comment"] = re.sub(' +', ' ', (str.strip(str.rstrip(ins)).replace('\n', ' ')))
        inspected_by = response.xpath("//div[@class='inspectionSummary__inspectorName']/p/text()").extract()
        if len(inspected_by):
            img_dict["Inspected by"] = re.sub(' +', ' ', (str.strip(str.rstrip(inspected_by[-1])).replace('\n', ' ')))
        yt_links = response.xpath("//iframe[@class='productDetails__body__youtubeFrame']/@src").extract()
        for index, yt_link in enumerate(yt_links):
            img_dict['YouTube ' + str(index + 1)] = yt_link
        ins_links = response.xpath("//a[@data-behavior='pdf-link-selector']/@href").extract()
        pre_image = pre_dictionary['Title']
        if 'Serial Number' in list(img_dict.keys()):
            pre_image = pre_image + '-' + img_dict['Serial Number']
        pre_image = unidecode.unidecode(pre_image)
        pre_image = pre_image.replace(' ', '-').replace(',', '-').replace('.', '-').replace('/', '-')
        pre_image = pre_image.replace('?', '-').replace('\\', '-').replace(':', '-').replace('*', '-')
        pre_image = pre_image.replace('<', '-').replace('>', '-').replace('|', '-').replace('\n', '-')
        pre_image = pre_image.replace('(', '-').replace(')', '-').replace('//', '-').replace('/', '-')
        for k in range(0, 50):
            pre_image = pre_image.replace('--', '-')
        if pre_image[0] == '-':
            pre_image = pre_image[1:]
        documents = response.xpath("//a[@class='dwn_att_pdf']/@href").extract()
        for index, doc_link in enumerate(documents):
            img_dict['Documents Link ' + str(index + 1)] = doc_link
            img_dict['Documents for this vehicle ' + str(index + 1)] = 'doc' + '-' + pre_image + '-' + str(index + 1) + '.pdf'

        for index, ins_link in enumerate(ins_links):
            img_dict['Inspection Link ' + str(index + 1)] = self.pre_link + ins_link
            img_dict['Inspection ' + str(index + 1)] = pre_image + '-' + str(index + 1) + '.pdf'

        for index, img_link in enumerate(img_links):
            img_name = pre_image + '-' + str(index + 1) + '.jpg'
            link_key = 'Image Link ' + str(index + 1)
            name_key = 'Image ' + str(index + 1)
            img_link = img_link.replace('/large/', '/zoom/')
            img_dict[name_key] = img_name
            img_dict[link_key] = img_link
        final_dictionary = dict(**final_dictionary, **img_dict)
        all_keys = list(final_dictionary.keys())
        final_dictionary = {k: [final_dictionary[k]] for k in all_keys}
        row = pd.DataFrame.from_dict(final_dictionary)
        self.final_df = pd.concat([self.final_df, row], axis=0, ignore_index=True)
