import pandas as pd
import datetime
import re
from django.utils.dateparse import parse_datetime
import matplotlib.pyplot as plt
import numpy as np
import matplotlib

# Change path
df = pd.read_csv("./resultsWithTopicDistribution.csv/part-00000-3fe56f1d-8a4d-4eac-b62c-2051f34a900c-c000.csv", header=None)
df.columns = ['docID', 'Probs', 'publishedAt', 'source']

# Forward fill the na values in dataframe
df.fillna(method='ffill', inplace=True)

# Handle and parse different datetime formats and return just date
def convertDateTimeToDate(x):
    dt = parse_datetime(x)
#     print(dt)
#     print(dt.date())
    return dt.date()

df.publishedAt = df.publishedAt.apply(convertDateTimeToDate)

# Convert the string of probabilities to list
def stringToList(s):
    s = s[1:-1]
    return s.split(',')

df.Probs = df.Probs.apply(stringToList)

def getListSeriesSplittedToDF(series):
    colnames = list()
    for i in range(len(series.values.tolist()[0])):
        colnames.append("Topic{}".format(i))
    return pd.DataFrame(series.values.tolist(), index=df.index, columns=colnames)

df = pd.concat([df, getListSeriesSplittedToDF(df.Probs)], axis=1)
df.to_csv('probs.csv')




# Change the path of the csv
new = pd.read_csv("./topicTerms.csv/part-00000-7a8849fc-0e30-4d57-8d4d-f8f8f59ab5b0-c000.csv", header=None)
new.columns = ['topicID', 'termWeights']
for cnt, s in enumerate(new.termWeights):
    ns = s[1:-1]
    x = re.findall(r"\('(.+?)',(.*?)\)", ns)
    word = list()
    score = list()
    for i in x:
        word.append(i[0])
        score.append(i[1])
    dataFrame = pd.DataFrame({"words" : word, "scores" : score})
    dataFrame.to_csv("topicNo{}.csv".format(cnt))

