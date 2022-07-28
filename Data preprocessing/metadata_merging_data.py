import os
import glob
import pandas as pd

"""
    Merging Science Direct files
    Collected Files exist in the folders:
    raw_ScienceDirect____ ScienceDirect___________ SD Disinfo: 10 files
                  |                  |_____ SD fake news: 10 files
                  |                  |_____ SD Spam: 10 files
                  |                  |_____ SD troll: 10 files
                  |__ ScienceDirect______ metadata_ScDr_misinformation.csv
                                     |__ metadata_ScDr_rumor.csv
"""
def merge_SD_multi(root_p, wanted_fields, wanted_size):
    """
    :param root_p: str of path
    :param wanted_fields: list
    :param wanted_size: integer
    :return: df
    """
    query_name = {
        'SD Disinfo': 'disinformation',
        'SD fake news': 'fake+news',
        'SD Spam': 'spam',
        'SD troll': 'troll'
    }
    all_data = []
    for fold in os.listdir(root_p):
        print(query_name[fold])
        data = []
        for file_p in glob.glob('%s\%s\*.csv' % (root_p, fold)):
            #             print(file_p)
            try:
                with open(file_p, 'r') as f:
                    df = pd.read_csv(f)
            except:
                with open(file_p, 'r', encoding='utf-8') as f:
                    df = pd.read_csv(f)
            data.append(df)
            dfs = pd.concat(data)
            dfs['query'] = query_name[fold]

            if wanted_size is not None:
                wanted_size = wanted_size
            else:
                wanted_size = len(dfs)
                # print('df.shape', dfs.shape)
            all_data.append(dfs[wanted_fields][:wanted_size])

    all_dfs = pd.concat(all_data)
    print('all_dfs.shape', all_dfs.shape)
    return all_dfs


def merge_SD_single(root_p, wanted_fields, wanted_size):
    data = []
    for file_p in glob.glob('%s\*.csv' % (root_p)):
        query = file_p.split('\\')[-1].split('_')[-1].split('.')[0]
        print(query)
        try:
            with open(file_p, 'r') as f:
                df = pd.read_csv(f)
        except:
            with open(file_p, 'r', encoding='utf-8') as f:
                df = pd.read_csv(f)
        df['query'] = query

        if wanted_size is not None:
            wanted_size = wanted_size
        else:
            wanted_size = len(df)
        data.append(df[wanted_fields][:wanted_size])
    dfs = pd.concat(data)
    print('dfs.shape', dfs.shape)
    return dfs



def merge_cross_DBs(root_p, wanted_fields, wanted_size=1000):
    """
    Merge data collected from the 4 databases
    :param root_p: str of path
    :param wanted_fields: list
    :param wanted_size: int
    :return: df
    """
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
    # Merge files from science direct database
    wanted_fields = ['Item type', 'Authors', 'Title', 'Journal', 'Publication year',
                     'Volume', 'Issue', 'Pages', 'Date published', 'ISSN', 'URLs', 'DOI',
                     'Abstract', 'Keywords', 'Notes', 'query']
    root = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata"
    out_p = root + '\merged_ScienceDirect'

    sd_multi_f = merge_SD_multi(
        root + "\\raw_ScienceDirect\\ScienceDirect", wanted_fields, wanted_size=None)
    sd_single_f = merge_SD_single(
        root + "\\raw_ScienceDirect\\Science_direct", wanted_fields, wanted_size=None)
    concat_SD = pd.concat([sd_multi_f, sd_single_f])
    print('the SD concatenated data has %d examples, and %d columns.' % (concat_SD.shape[0], concat_SD.shape[1]))
    print(concat_SD.columns)
    try:
        os.makedirs(out_p, exist_ok=True)
    except OSError as error:
        print('Directory cannot be created!')
    with open(out_p + '\ScienceDirect.csv', 'w', encoding='utf-8', newline='') as f:
        concat_SD.to_csv(f)

    # Merge files from the 4 databases into 1 single csv file
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