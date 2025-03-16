import os
import numpy as np
import pandas as pd

if __name__ == "__main__":
    sample_path = '/home/faizan/web_scraping/Cognitia/bas-scraping/BAS/Sample_Client.xlsx'
    equippo_sel_path = '/home/faizan/web_scraping/Cognitia/equippo/equippo_data/equippo.xlsx'
    equippo_scr_path = '/home/faizan/web_scraping/Cognitia/equippo/equippo_data/scrapy_equippo.xlsx'
    sample_df = pd.read_excel(sample_path)
    
    sel_df = pd.read_excel(equippo_sel_path)
    sel_df.dropna(axis=1, how='all', inplace=True)
    del sel_df['Inspection Link S3']

    scr_df = pd.read_excel(equippo_scr_path)
    scr_cols = list(scr_df.columns)
    for col in scr_cols:
        if 'Image Link' in col:
            del scr_df[col]
        if 'Documents Link' in col:
            del scr_df[col]

    scr_cols = list(scr_df.columns)
    img_cols, docs_cols = [], []
    for col in scr_cols:
        if 'Image' in col:
            img_cols.append(col)
        if 'Documents for this vehicle' in col:
            docs_cols.append(col)
    
    scr_mapping = {'Serial Number': 'Serial', 'YouTube 1': 'YouTube', 'Price': 'RRP', 'Sub Title': 'Sub title', 
            'Hours': 'Engine Hours', 'Kilometers': 'Mileage'}
    scr_df.rename(columns=scr_mapping, inplace=True)
    sel_mapping = {'Engine power': 'Max HP', 'Brand & model': 'Model'}
    sel_df.rename(columns=sel_mapping, inplace=True)

    df = pd.merge(scr_df, sel_df, on=['URL'])
    for i in range(0, len(df)):
        if 'ID ' in df['ID'].iloc[i]:
            df['ID'].iloc[i] = df['ID'].iloc[i].split('ID ')[1]

    sample_cols = list(sample_df.columns)
    dealer_sample = ['Dealer Name', 'Dealer Country', 'Dealer Logo', 'Contact', 'Mobile No.', 'Contact Email', 'Contact Language']
    specs_cols = sample_cols[:25]
    dealer_cols = []
    for i in range(1, 3):
        for col in dealer_sample:
            dealer_cols.append(col + ' ' + str(i))
    meta_cols = sample_cols[32:34]
    documents_cols = sample_cols[34:48]
    if len(docs_cols) < len(documents_cols):
        docs_cols = documents_cols
    youtube_cols = sample_cols[48:52]
    extra_cols = sample_cols[102:]

    arranged_cols = []
    arranged_cols.extend(specs_cols)
    arranged_cols.extend(dealer_cols)
    arranged_cols.extend(documents_cols)
    arranged_cols.extend(youtube_cols)
    arranged_cols.extend(img_cols)
    arranged_cols.extend(extra_cols)

    df_cols = list(df.columns)
    arranged_cols.extend(list(set(df_cols) - set(arranged_cols)))
    for col in arranged_cols:
        if col not in df_cols:
            df[col] = ''
    df[arranged_cols].to_excel('/home/faizan/web_scraping/Cognitia/equippo/equippo_data/equippo_17_03_2022.xlsx', index=False)
