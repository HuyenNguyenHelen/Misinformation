# from bs4 import BeautifulSoup
import requests, json, csv
import pandas as pd
import re

def scrape_meta_by_query (query, start_from, no_limit, fields):
    headers = create_headers()
    url = 'https://api.semanticscholar.org/graph/v1/paper/search?query={}&offset={}&limit={}&fields={}'.format(
        query, start_from, no_limit, fields)
    data = requests.get(url, allow_redirects=True, headers=headers).text
    dic_data = json.loads(data)
    no_total = dic_data['total']
    offset = dic_data['offset']
    df = pd.json_normalize(dic_data['data'])
    return no_total, offset, df


def main_by_query ():
    fields = 'title,authors,abstract,venue,year,citationCount,fieldsOfStudy'
    queries = ['misinformation', 'disinformation', 'spam', 'fake+news', 'rumor', 'troll']
    for query in queries:
      byquery = []
      print('--------------%s---------------------'%query)
      start_from = 0
      no_limit = 99
      total = no_limit+1
      while start_from + no_limit < 1000:
        no_total, offset, df = scrape_meta_by_query(query = query, start_from = start_from, no_limit = no_limit, fields = fields)
        start_from += no_limit
        total = no_total
        print('start_from: ', start_from)
        print('no_total: ', no_total)
        byquery.append(df)
      df_byquery = pd.concat(byquery)
      with open(r'C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\metadata\metadata_sem-scholar_%s.csv'%query,
                'w', encoding = 'utf-8', newline = '') as f:
        df_byquery.to_csv(f)


def create_headers():
    APIkey = 'Z7TWzEV1Wb5PBEjBkp3rL30fdqqEmfb0501oqtLb'
    headers = {"x-api-key": "{}".format(APIkey)}
    return headers


def scrape_by_doi(doi, fields, wanted_field):
    headers = create_headers()
    url = 'https://api.semanticscholar.org/graph/v1/paper/{}?fields={}'.format(doi, fields)
    data = requests.get(url, allow_redirects=True, headers=headers).text
    dic_data = json.loads(data)
    print(dic_data)
    if 'error' not in dic_data.keys():
        return dic_data[wanted_field]
    else:
        return ''

'''
 # wanted_content = []
        # for doi in data_no_wanted.doi:
        #     if isinstance(doi, str) and len(doi)>1:
        #         wanted_temp = scrape_by_doi(doi = doi, fields = wanted_field, wanted_field = wanted_field)
        #         wanted_content.append(wanted_temp)
        #     else:
        #         wanted_content.append('')
        # data_no_wanted['abstract'] = wanted_content
'''


def main_by_doi(data_p, out_p, wanted_field):
    with open(data_p, 'r', encoding = 'utf-8') as f:
        data = pd.read_csv(f)
    data = data[:100]
    print(data.columns)
    for field in wanted_field.split(','):
        print(field)
        data_with_wanted = data[data[field].notnull()]
        print('data_with_wanted', data_with_wanted.shape)
        print(data_with_wanted.head(3))

        data_no_wanted = data[data[field].isnull()]
        print('data_without_wanted', data_no_wanted.shape)
        print(data_no_wanted.head(3))

        wanted_content = []
        for doi in data_no_wanted.doi:
            if isinstance(doi, str) and len(doi)>1:
                wanted_temp = scrape_by_doi(doi = doi, fields = wanted_field, wanted_field = field )
                wanted_content.append(wanted_temp)
            else:
                wanted_content.append('')
        data_no_wanted[field] = wanted_content

        concat_data = pd.concat([data_with_wanted, data_no_wanted])
        print('The final data has shape:', concat_data.shape)
        print('Data with wanted:', concat_data[field].notnull().value_counts())
        data = concat_data

    with open(out_p, 'w', encoding = 'utf-8', newline='') as f:
        concat_data.to_csv(f)


def scrape_venue_by_title(title, n_limit, fields):
    headers = create_headers()
    url = 'https://api.semanticscholar.org/graph/v1/paper/search?query={}&limit={}&fields={}'.format(
        title, n_limit, fields)
    data = requests.get(url, allow_redirects=True, headers=headers).text
    dic_data = json.loads(data)
    if dic_data['data']:
        print(dic_data['data'][0])
        return dic_data['data'][0]['venue'], dic_data['data'][0]['title']
    else:
        return '', ''


def main_by_title():
    data_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\null_venues_filled-by-doi.csv"
    with open(data_p, 'r', encoding='utf-8') as f:
        data = pd.read_csv(f)
    data_copy = data.copy()
    print(data_copy.columns)
    count_null = 0
    venues = []
    for i in range(len(data_copy)):
        print('---------------------------------------------------')
        if isinstance(data_copy['venue'][i], float) or len(data_copy['venue'][i])<1:
            title = data_copy['title'][i].lower()
            print(title)
            title_cleaned = title.replace(' ', '+').replace(',', '').replace(':', '').replace('-', '+')
            try:
                venue, re_title = scrape_venue_by_title(title=title_cleaned, n_limit=1, fields = 'title,venue')
                venues.append(venue)
                print('....successful.....\n', venue)
                count_null += 1
            except:
                venues.append('')
                print('fail!\n')
            # if re.findall('[a-z]+', re_title) == re.findall('[a-z]+', title):
            #     print(title, '<-match->', re_title)
            #     venues.append(venue)
            # else:
            #     print(title, '>-not-match-<', re_title)
            #     venues.append('')
        else:
            venues.append(data_copy['venue'][i])
            print('already got venues: ', data_copy['venue'][i])
    print(count_null)
    data_copy = data_copy.drop(columns=['venue'])
    data_copy['venue'] = venues
    out_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\null_venues_filled-by-titles.csv"
    with open(out_p, 'w', encoding='utf-8', newline='') as f:
        data_copy.to_csv(f)


if __name__ == '__main__':
    # main_by_query()
    # main_by_doi (data_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal+doi+abstract_data.csv",
    #             out_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal+doi+abstract+year+citation+fieldofstudy.csv", 
    #             wanted_field = 'citationCount,fieldsOfStudy,year')

    main_by_doi(data_p=r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal_data.csv",
                out_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal_data+abstract_filled.csv", 
                wanted_field = 'abstract')
    # main_by_title()


