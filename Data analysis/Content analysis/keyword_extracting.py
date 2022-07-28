# Import essential libraries
import pandas as pd
from wordcloud import WordCloud, STOPWORDS
from nltk.stem import WordNetLemmatizer
import yake

def yake_extractor(data_list, lemmatizer, stopwords, language = "en", max_ngram_size = 3, deduplication_thresold = 0.9, 
    deduplication_algo = 'seqm', windowSize = 1, numOfKeywords =40):
    # preprocess data
    lem_data = [lemmatizer.lemmatize(data) for data in data_list]
    text = []
    for data in lem_data:
        text.append(' '.join([w for w in data.split() if w not in stopwords else w]))
    text = ' '.join(text)
    # extract keywords
    custom_kw_extractor = yake.KeywordExtractor(lan=language, n=max_ngram_size,  dedupLim=deduplication_thresold,
                            dedupFunc=deduplication_algo, windowsSize=windowSize, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    return keywords

if __name__ == '__main__':
    stopwords = set(STOPWORDS)
    lemmatizer = WordNetLemmatizer()
    data_path = 'r/....'
    out_path = ''
    with open(data_path, 'r') as f:
        data = pd.read_csv(f)
    print(data.columns)
    join_fn = lambda x: ' '.join([x.title, x.abstract])
    join_text = data.apply(join_fn, axis=1)
    keywords = yake_extractor(data_list = join_text.tolist(), lemmatize = lemmatizer, stopwords = stopwords)
    f = open(out_path, 'w+')
    f.writelines(keywords)    
