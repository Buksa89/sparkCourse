from pyspark import SparkConf, SparkContext
import re
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag

stop_words = stopwords.words('english')
lemmatizer = WordNetLemmatizer()
WORD_RE = re.compile(r'[\w]+')

conf = SparkConf().setMaster("local").setAppName("reviews")
sc = SparkContext(conf = conf)
# data = sc.textFile("files/prep_reviews_sample.tsv")
data = sc.textFile("files/prep_reviews.tsv")

def get_data_mapper(line):
    (Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,
     HelpfulnessDenominator,Score,Time,Summary,Text) = line.split('\t')
    words = WORD_RE.findall(Text)
    words = list(filter(lambda word: len(word)>1 , words))
    words = map(str.lower, words)
    words = map(lemmatizer.lemmatize, words)
    words = filter(lambda word: word not in stop_words, words)
    words = list(filter(lambda word: pos_tag([word])[0][1]=='JJ', words))
    Score = int(Score)
    
    return map(lambda word: ((Score, word),1),words)

def get20words(x,y):
    list20words = x[0]
    new_word = y[1]
    
    if len(list20words) <20:
        list20words.append(new_word)
    elif new_word > min(list20words):
        min_value_pos = list20words.index(min(list20words))
        list20words[min_value_pos] = new_word
    
    return [list20words]

rdd = data.flatMap(get_data_mapper)
rdd = rdd.reduceByKey(lambda x,y: x+y)
rdd = rdd.filter(lambda x:  x[0][0] == 5 or x[0][0] == 1)
rdd = rdd.map(lambda x: (x[0][0], [[],[x[1],x[0][1]]]))
rdd = rdd.reduceByKey(get20words)
rdd = rdd.mapValues(lambda x: x[0])
# print(rdd.collect())
for key, value in rdd.collect():
        print (f'{key}\t{value}')