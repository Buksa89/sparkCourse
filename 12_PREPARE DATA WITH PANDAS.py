import pandas as pd
 
df=pd.read_csv("files/Reviews.csv")

df.to_csv('files/prep_reviews.tsv', sep='\t', header=False, index=False)