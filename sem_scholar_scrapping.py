# from bs4 import BeautifulSoup
import requests, json, csv
import pandas as pd

def scrape_meta_by_query (query, start_from, no_limit, fields):
    url = 'https://api.semanticscholar.org/graph/v1/paper/search?query={}&offset={}&limit={}&fields={}'.format(
        query, start_from, no_limit, fields)
    data = requests.get(url, allow_redirects=True).text
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
    headers = {"Authorization": "{}".format(APIkey)}
    return headers


def scrape_venue_by_doi(doi, fields):
    headers = create_headers()
    url = 'https://api.semanticscholar.org/graph/v1/paper/{}?fields={}'.format(doi, fields)
    data = requests.get(url, allow_redirects=True, headers=headers).text
    dic_data = json.loads(data)
    print(dic_data)
    if 'error' not in dic_data.keys():
        return dic_data['venue']
    else:
        return ''
    # no_total = dic_data['total']
    # offset = dic_data['offset']
    # df = pd.json_normalize(dic_data['data'])
    # return dic_data['venue']


def main_by_doi ():
    data_p = r"C:\Users\huyen\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\null_venues.csv"
    with open(data_p, 'r', encoding = 'utf-8') as f:
        data = pd.read_csv(f)
    data_copy = data.copy()
    data_copy['venue'] = data_copy['doi'].apply(lambda x: scrape_venue_by_doi(doi = x, fields = 'venue') if isinstance(x, str) else None)
    print(data_copy['venue'].value_counts() )
    # for doi in data.doi:
    #     if len(doi)>0:
    #         venue = scrape_venue_by_doi(doi = doi, fields = 'venue')
    out_p = r"C:\Users\huyen\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\null_venues_filled-by-doi.csv"
    with open(out_p, 'w', encoding = 'utf-8') as f:
        data_copy.to_csv(f)

if __name__ == '__main__':
    # main_by_query()
    main_by_doi()

