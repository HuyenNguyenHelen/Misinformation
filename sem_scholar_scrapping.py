# from bs4 import BeautifulSoup
import requests, json, csv
import pandas as pd

def scrape_metadata(query, start_from, no_limit, fields):
  url = 'https://api.semanticscholar.org/graph/v1/paper/search?query={}&offset={}&limit={}&fields={}'.format(
      query, start_from, no_limit, fields)
  data = requests.get(url, allow_redirects=True).text
  dic_data = json.loads(data)
  no_total = dic_data['total']
  offset = dic_data['offset']
  df = pd.json_normalize(dic_data['data'])
  return no_total, offset, df


def main ():
  fields = 'title,authors,abstract,venue,year,citationCount,fieldsOfStudy'
  queries = ['misinformation', 'disinformation', 'spam', 'fake+news', 'rumor', 'troll']
  for query in queries:
    byquery = []
    print('--------------%s---------------------'%query)
    start_from = 0
    no_limit = 99
    total = no_limit+1
    while start_from + no_limit < 1000:
      no_total, offset, df = scrape_metadata(query = query, start_from = start_from, no_limit = no_limit, fields = fields)
      start_from += no_limit
      total = no_total
      print('start_from: ', start_from)
      print('no_total: ', no_total)
      byquery.append(df)
    df_byquery = pd.concat(byquery)
    with open(r'C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\metadata\metadata_sem-scholar_%s.csv'%query,
              'w', encoding = 'utf-8', newline = '') as f:
      df_byquery.to_csv(f)

main()