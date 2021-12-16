# PRzekstałcenie słów do bezokoliczników
from nltk.stem import WordNetLemmatizer
lemmatizer = WordNetLemmatizer()
# print(lemmatizer.lemmatize('mice'))

# Lista słów które nie mają znaczenia w liczeniu ich ilości 
from nltk.corpus import stopwords
stop_words = stopwords.words('english')
# print(stop_words)

# funkcja sprawdza jakim typem jest dane słowo (rzeczownik, przymiotnik itp)
from nltk import pos_tag

lst = ['seems', 'little', 'wholesome', 'supermarket', 'brand', 'somewhat', 'mushy', 'quite', 'much', 'flavor', 'either', 'pas', 'muster', 'kid', 'probably', 'buy', 'flavor', 'good', 'however', 'see', 'differce', 'oaker', 'oat', 'brand', 'mushy', 'stuff', 'buy', 'big', 'box', 'store', 'nothing', 'healthy', 'carbs', 'sugar', 'save', 'money', 'get', 'something', 'least', 'ha', 'taste']
print (lst)
print('---')
print ()
# lst = list(filter(lambda word: True, lst))


# # print (pos_tag(['big']))