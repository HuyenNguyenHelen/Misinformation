# Import essential libraries
import pandas as pd
from wordcloud import WordCloud, STOPWORDS
from nltk.stem import WordNetLemmatizer
import yake
import nltk
# nltk.download('omw-1.4')

def yake_extractor(data_list, lemmatizer, stopwords, language = "en", max_ngram_size = 3, deduplication_thresold = 0.9, 
    deduplication_algo = 'seqm', windowSize = 1, numOfKeywords =100):
    # preprocess data
    lower_data = [data.lower() for data in data_list]
    lem_data = [lemmatizer.lemmatize(data) for data in lower_data]
    # text = []
    # for data in lem_data:
    #     text.append(' '.join([w for w in data.split() if w not in stopwords]))
    text = ' '.join(lem_data)
    # extract keywords
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size,  dedupLim=deduplication_thresold,
                            dedupFunc=deduplication_algo, windowsSize=windowSize, top=numOfKeywords, features=None, stopwords=stopwords)
    keywords = custom_kw_extractor.extract_keywords(text)
    return keywords

if __name__ == '__main__':
    stopwords = set(STOPWORDS)
    lemmatizer = WordNetLemmatizer()

    data_path = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal+doi+abstract+year+citation+fieldofstudy_dropnull.csv"
    out_path = r'C:\Users\hn0139\Documents\GitHub\Misinformation\Data analysis\Content analysis\keyword.csv'
    
    with open(data_path, 'r', encoding = 'utf-8') as f:    
        data = pd.read_csv(f)
    print(data.columns)

    join_fn = lambda x : ' '.join([x.title, x.abstract])
    join_text = data.apply(join_fn, axis=1)
    keywords = yake_extractor(data_list = join_text.tolist(), lemmatizer = lemmatizer, stopwords = stopwords)
    
    # write output to file
    f = open(out_path, 'w')
    f.writelines('keyword,score\n')
    for (w, s) in keywords: 
        f = open(out_path, 'a+')
        f.write('%s,%f\n' %(w,s))   
    f.close()
