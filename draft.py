def merge_cross_DBs(root_p, wanted_fields, wanted_size=1000):
    fold_names = {
        'scopus': 'Scopus csv',
        'semantic_scholar': "Semantic_scholar",
        'wos': "WoS_Metadata",
        'science_direct': 'merged_ScienceDirect'
    }

    all_data = []
    for k, v in fold_names.items():
        print('\n_____%s_____' % k)
        fold_p = root_p + "\\" + v
        file_ps = [f for f in glob.glob(fold_p + '\*.csv')]
        if k == 'science_direct':
            if len(file_ps) == 1:
                with open(file_ps[0], 'r', encoding='utf-8') as f:
                    df = pd.read_csv(f)
            else:
                print('more than one file in the directory')
            df.columns = ['Item type', 'authors', 'title', 'venue', 'year', 'Volume', 'Issue', 'Pages',
                          'Date published', 'Date published',
                          'ISSN', 'URLs', 'doi', 'abstract', 'Keywords', 'Notes', 'query']
            df['database'] = k
            new_cols = list(set(wanted_fields).difference(list(df.columns)))
            for col in new_cols:
                df[col] = ''
            df = df[wanted_fields]
            all_data.append(df)
        else:
            for path in file_ps:
                try:
                    with open(path, 'r') as f:
                        df = pd.read_csv(f)
                except:
                    with open(path, 'r', encoding='utf-8') as f:
                        df = pd.read_csv(f)
                if wanted_size > len(df):
                    wanted_size = 1000
                else:
                    wanted_size = len(df)
                print('wanted_size: %d'%wanted_size)
                if k == 'scopus':
                    df.columns = ['Item type', 'authors', 'title', 'venue', 'year', 'Volume',
                                  'Issue', 'Pages', 'Date published', 'URLs', 'doi', 'Notes', 'citationCount']

                    query = path.split('\\')[-1].split(' ')[-1].split('.')[0]
                elif k == 'wos':
                    df.columns = ['Publication Type', 'authors', 'Book Authors', 'Book Editors',
                                  'Book Group Authors', 'Author Full Names', 'Book Author Full Names',
                                  'Group Authors', 'title', 'Source Title', 'Book Series Title',
                                  'Book Series Subtitle', 'Language', 'Document Type', 'Conference Title',
                                  'Conference Date', 'Conference Location', 'Conference Sponsor',
                                  'Conference Host', 'Author Keywords', 'Keywords Plus', 'abstract',
                                  'Addresses', 'Affiliations', 'Reprint Addresses', 'Email Addresses',
                                  'Researcher Ids', 'ORCIDs', 'Funding Orgs', 'Funding Name Preferred',
                                  'Funding Text', 'Cited References', 'Cited Reference Count',
                                  'citationCount, WoS Core', 'Times Cited, All Databases',
                                  '180 Day Usage Count', 'Since 2013 Usage Count', 'venue',
                                  'Publisher City', 'Publisher Address', 'ISSN', 'eISSN', 'ISBN',
                                  'Journal Abbreviation', 'Journal ISO Abbreviation', 'Publication Date',
                                  'year', 'Volume', 'Issue', 'Part Number', 'Supplement',
                                  'Special Issue', 'Meeting Abstract', 'Start Page', 'End Page',
                                  'Article Number', 'doi', 'DOI Link', 'Book DOI', 'Early Access Date',
                                  'Number of Pages', 'WoS Categories', 'Web of Science Index',
                                  'fieldsOfStudy', 'IDS Number', 'Pubmed Id', 'Open Access Designations',
                                  'Highly Cited Status', 'Hot Paper Status', 'Date of Export',
                                  'UT (Unique WOS ID)', 'Web of Science Record']
                    query = path.split('\\')[-1].split('_')[-1].split('.')[0]

                elif k == 'semantic_scholar':
                    query = path.split('\\')[-1].split('_')[-1].split('.')[0]
                else:
                    pass
                print('%s 11111 has %d instances.' % (k, df.shape[0]))
                df = df[:wanted_size]
                df['query'] = query

                df['database'] = k
                new_cols = list(set(wanted_fields).difference(list(df.columns)))
                for col in new_cols:
                    df[col] = ''
                df = df[wanted_fields]
                all_data.append(df)
                print('%s 2222 has %d instances.' % (k, df.shape[0]))

    all_dfs = pd.concat(all_data)
    print('\nDone! Merged df has %s examples, and %s fields' % (str(all_dfs.shape[0]), str(all_dfs.shape[1])))
    return all_dfs


if __name__ == "__main__":
    root_p = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata"
    wanted_fields = ['title', 'authors', 'venue', 'year', 'citationCount', 'fieldsOfStudy', 'abstract', 'doi', 'query',
                     'database']
    merged_all_data = merge_cross_DBs(root_p, wanted_fields, wanted_size=1000)

    out_p = root_p + '\merged_all_data'
    try:
        os.makedirs(out_p, exist_ok=True)
    except OSError as error:
        print('Directory cannot be created!')
    with open(out_p + '\merged_all_data.csv', 'w', encoding='utf-8', newline='') as f:
        merged_all_data.to_csv(f)