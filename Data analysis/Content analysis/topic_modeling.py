# Import essential libraries
import pandas as pd
from nltk.tokenize import RegexpTokenizer
# from stop_words import get_stop_words
from wordcloud import WordCloud, STOPWORDS

from nltk.stem.porter import PorterStemmer
import gensim
import gensim.corpora as corpora
from gensim.models import CoherenceModel


def preprocessing(tokenizer, text):
    """
    text: str
    """
    stopwords = set(STOPWORDS)
    text = text.lower()
    tokens = tokenizer.tokenize(text)
    # Remove stop words from tokens
    stopped_tokens = [i for i in tokens if not i in stopwords]
    # Stem tokens
    stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens if len(i)>3]
    return stemmed_tokens


def get_lda_topic(texts, n_topics =[20, 25, 30, 35, 40], n_words = 12):
    """
    texts: a list of token lists
    """
    # Turn our tokenized documents into a id - term dictionary
    dictionary = corpora.Dictionary(texts)
    # Convert tokenized documents into a document-term matrix
    corpus = [dictionary.doc2bow(text) for text in texts]

    # Find the best n_topics
    models = {}
    topics = {} 
    coherence = {}
    for i in n_topics:
        # Generate LDA model
        print('--------- %s ---------'%str(i))
        ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=i, id2word = dictionary, passes=60, alpha='auto', random_state=42)
        shown_topics = ldamodel.show_topics(num_topics=i, 
                                            num_words=n_words,
                                            formatted=False)
        topics[i] = [[word[0] for word in topic[1]] for topic in shown_topics]
        # Compute Coherence Score
        coherence_ldamodel = CoherenceModel(model=ldamodel, texts=texts, dictionary=dictionary, coherence='c_v')
        coherence_lda = coherence_ldamodel.get_coherence()
        coherence[i] = coherence_lda
        print('Coherence Score: ', coherence_lda)

    return topics, coherence


if __name__ == '__main__':
    # Create Tokenizer
    tokenizer = RegexpTokenizer(r'\w+')
    # Create PorterStemmer
    p_stemmer = PorterStemmer()

    data_path = r"C:\Users\hn0139\OneDrive - UNT System\A_PhD_PATH\PROJECTS\Misinformation\Misinformation_literature_review\metadata\merged_all_data\journal+doi+abstract+year+citation+fieldofstudy_dropnull.csv"
    out_path = r'C:\Users\hn0139\Documents\GitHub\Misinformation\Data analysis\Content analysis\topics.csv'
    
    with open(data_path, 'r', encoding = 'utf-8') as f:    
        data = pd.read_csv(f)

    join_fn = lambda x : ' '.join([x.title, x.abstract])
    join_text = data.apply(join_fn, axis=1)

    clean_text = [preprocessing(tokenizer, text) for text in join_text]
    
    topics, coherences = get_lda_topic (clean_text, n_topics = [30], n_words = 10) # 25, 30, 35, 40
    best_cor_score = max(list(coherences.values()))
    best_cor_idx = list(coherences.values()).index(best_cor_score)
    best_n_topics = {v:k for k, v in coherences.items()}[best_cor_score]

    best_topics = pd.DataFrame(topics[best_n_topics])
    print('best n_topic: {} topics. Cohenrence score: {} \nTopics: \n{}'.format(best_n_topics, coherences, best_topics))
    with open(out_path, 'w',  newline="", encoding='utf-8') as file:
        best_topics.to_csv(file)