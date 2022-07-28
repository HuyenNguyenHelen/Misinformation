

import json
import requests

## Load configuration
con_file = open("config_scopusAPI.json")
config = json.load(con_file)
con_file.close()



def create_headers():
    con_file = open("config_scopusAPI.json")
    config = json.load(con_file)
    con_file.close()
    APIkey = config['apikey']
    headers = {"X-ELS-APIKey": "{}".format(APIkey)}
    return headers

def scrape_venue_by_title(db, title, fields = ''):
    headers = create_headers()
    url = 'https://api.elsevier.com/content/search/{}?query={}&fields={}&count=100'.format(db,
        title, fields)
    data = requests.get(url, allow_redirects=True, headers=headers).text
    dic_data = json.loads(data)
    print(dic_data['search-results']['entry'][0].keys())
    if dic_data['search-results']['entry']:
        title = dic_data['search-results']['entry'][0]['dc:title']
        venues =  dic_data['search-results']['entry'][0]['prism:publicationName']
        pub_type = dic_data['search-results']['entry'][0]['prism:aggregationType']
        return title, venues, pub_type
    else:
        return '', '', ''




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


# get_venue_publication_type('Protecting Democracy from Disinformation: Normative Threats and Policy Responses')
print(scrape_venue_by_title('scopus', title='Detection of cyber-aggressive comments on social media networks: A machine learning and text mining approach',
                     fields = 'title,doi,pubType,publicationName'))

'dc:title,prism:doi,pubType,prism:publicationName'