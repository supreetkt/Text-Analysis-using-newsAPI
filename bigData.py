cluster_seeds = ['199.60.17.171', '199.60.17.188']
import nltk
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import Tokenizer, StopWordsRemover, VectorAssembler, HashingTF, IDF
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from nltk.corpus import stopwords
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.clustering import LDA
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer
from pyspark.sql.types import StructType, StructField, StringType
from stop_words import get_stop_words
import pyspark_cassandra
import re, sys, math, string
import nltk

conf = SparkConf().setAppName('news-data').set('spark.cassandra.connection.host', ','.join(cluster_seeds))
sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# assert sys.version_info >= (3, 4)  # make sure we have Python 3.4+
# assert spark.version >= '2.2'  # make sure we have Spark 2.2+

# List of additional stopwords
stplist = ['a', "a's", 'able', 'about', 'above', 'according', 'accordingly', 'across', 'actually', 'after', 'afterwards', 'again', 'against', "ain't", 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', "aren't", 'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'b', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'c', "c'mon", "c's", 'came', 'can', "can't", 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'co', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', "couldn't", 'course', 'currently', 'd', 'definitely', 'described', 'despite', 'did', "didn't", 'different', 'do', 'does', "doesn't", 'doing', "don't", 'done', 'down', 'downwards', 'during', 'e', 'each', 'edu', 'eg', 'eight', 'either', 'else', 'elsewhere', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'f', 'far', 'few', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'four', 'from', 'further', 'furthermore', 'g', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'h', 'had', "hadn't", 'happens', 'hardly', 'has', "hasn't", 'have', "haven't", 'having', 'he', "he's", 'hello', 'help', 'hence', 'her', 'here', "here's", 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'i', "i'd", "i'll", "i'm", "i've", 'ie', 'if', 'ignored', 'immediate', 'in', 'inasmuch', 'inc', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'insofar', 'instead', 'into', 'inward', 'is', "isn't", 'it', "it'd", "it'll", "it's", 'its', 'itself', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'know', 'knows', 'known', 'l', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', "let's", 'like', 'liked', 'likely', 'little', 'look', 'looking', 'looks', 'ltd', 'm', 'mainly', 'many', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'merely', 'might', 'more', 'moreover', 'most', 'mostly', 'much', 'must', 'my', 'myself', 'n', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'o', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'p', 'particular', 'particularly', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provides', 'q', 'que', 'quite', 'qv', 'r', 'rather', 'rd', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'she', 'should', "shouldn't", 'since', 'six', 'so', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 't', "t's", 'take', 'taken', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', "that's", 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', "there's", 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 'these', 'they', "they'd", "they'll", "they're", "they've", 'think', 'third', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twice', 'two', 'u', 'un', 'under', 'unfortunately', 'unless', 'unlikely', 'until', 'unto', 'up', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'uucp', 'v', 'value', 'various', 'very', 'via', 'viz', 'vs', 'w', 'want', 'wants', 'was', "wasn't", 'way', 'we', "we'd", "we'll", "we're", "we've", 'welcome', 'well', 'went', 'were', "weren't", 'what', "what's", 'whatever', 'when', 'whence', 'whenever', 'where', "where's", 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', "who's", 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', "won't", 'wonder', 'would', 'would', "wouldn't", 'x', 'y', 'yes', 'yet', 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves', 'z', 'zero']


spark = SparkSession.builder.master("local").appName('lda').getOrCreate()
sc = spark.sparkContext

# schema = StructType([ StructField('author', StringType(), True),
#                     StructField('description', StringType(), True),
#                     StructField('publishedat', StringType(), True),
#                     StructField('source', StringType(), True),
#                     StructField('title', StringType(), True),
#                     StructField('url', StringType(), True),
#                     StructField('urltoimage', StringType(), True) ])

# newsData = spark.read.csv('/Users/vishalshukla/Downloads/news.csv', schema)
df_input.createOrReplaceTempView('newsdata')
df_input = spark.sql('''SELECT * from newsdata
WHERE source in ('engadget', 'hacker-news', 'techcrunch', 'techradar', 'the-verge')
''')

'''
('bbc-news', 'cnbc', 'cnn', 'the-new-york-times', 'the-wall-street-journal', 'the-huffington-post', 'the-washington-post')

sports
('bbc-sport', 'espn', 'the-sport-bible', 'four-four-two', 'fox-sports', 'talksport')

technology
('engadget', 'hacker-news', 'techcrunch', 'techradar', 'the-verge')

business
('bloomberg', 'business-insider', 'cnbc', 'fortune', 'the-wall-street-journal')



'''

print(df_input.show())

english_stopwords = get_stop_words(language='english') #stopwords.words("english")
english_stopwords+=stplist
sc.broadcast(english_stopwords)
keyspace = 'vshukla'
table = 'newsdata'
# outputDirectory = 'aa56'  # home/stakkar/
lemma = WordNetLemmatizer()
ps = PorterStemmer()

def lemmatizer(strinput):
    # Function to lemmatize list of words in the output of Tokenizer
    outList = list()
    lemma = WordNetLemmatizer()
    for i in strinput:
        outList.append(lemma.lemmatize(i))
    return outList

# UDF for lemmatizer
# lemmatizationStep = udf(lemmatizer, StringType())

def performStemming(strinput):
    # Function to perform stemming using Porterstemmer
    ps = PorterStemmer()
    outList = list()
    for i in strinput:
        outList.append(ps.stem(i))
    return outList

# UDF for stemmer
# stemmingStep = udf(performStemming, StringType())

def indices_to_terms(vocabulary):
    def indices_to_terms(xs):
        return [vocabulary[int(x)] for x in xs]
    return udf(indices_to_terms, ArrayType(StringType()))

def zipTermIndicesWordsWeights(x):
    xlist = list()
    for i in zip(x[1],x[3],x[2]):
        xlist.append(i)
    return ((x[0], xlist))



def zipTermsWeights(col1, col2):
    outList = list()
    for i in zip(col1, col2):
        outList.append(i)
    return (str(outList))

zipperUDF = udf(zipTermsWeights, StringType())


def cleanUp(text):
    # nltk.data.path.append('/home/vshukla/nltk_data')
    # Removing all special characters / non-alphanumeric characters
    text = re.sub('\W+', ' ', str(text))
    # Removing digits, converting to lowercase
    cleanerText = re.sub(r'[0-9]+', '', str(text)).lower()
    #lemmatization
    normalized = " ".join(lemma.lemmatize(word) for word in cleanerText.split())
    # Removing stop-words
    stop_words_free = " ".join([i for i in normalized.split() if i not in english_stopwords and len(i) > 3])
    #Stemming
    # stemmed = " ".join([ps.stem(i) for i in stop_words_free.split()])
    return (stop_words_free)#stop_words_free)

cleanupUDF = udf(cleanUp, StringType())

def convertArraytoString(inp):
    return (str(inp))

arrayStringUDF = udf(convertArraytoString, StringType())

def df_for(keyspace, table, split_size):
    df = spark.createDataFrame(sc.cassandraTable(keyspace, table, split_size=split_size).setName(table))
    df.createOrReplaceTempView(table)
    return df


# fetch data from cassandra table newsdata
df_input = df_for(keyspace, table, 100)
df_input.createOrReplaceTempView('newsdata')
df_input = spark.sql('''SELECT * from newsdata
WHERE source in ('engadget', 'hacker-news', 'techcrunch', 'techradar', 'the-verge')
''')

# concatenate title and description because we will perfrom pre-processing on it
rdd_input = df_input.select(concat(df_input.title, lit(" "), df_input.description) \
                            .alias('Text'), df_input.publishedat, df_input.source)

# .rdd.map(lambda x: (x['Text'], x['publishedat'], x['source']))

print(rdd_input.show())

# perform cleaning - removed numeric characters, punctuations
cleaned_df = rdd_input.select(rdd_input['Text'], cleanupUDF(rdd_input['Text']).alias('CleanedText'), rdd_input.publishedat, rdd_input.source)
# cleaned_rdd = rdd_input.map(cleanUp).map(lambda line: line.split(","))

# converted to dataframes again for tokenizer operations
# df = cleaned_rdd.toDF()
print(cleaned_df.show())

# tokenizer
tokenizer = Tokenizer(inputCol='CleanedText', outputCol='Words')
# countTokens = udf(lambda Words: len(Words), IntegerType())
tokenized = tokenizer.transform(cleaned_df)
# words_df = tokenized.select("Words")


# TF-IDF
'''
Hashing TF misses the vocabulary which is essential for techniques like LDA. For this one has to use CountVectorizer function. Irrespective of the vocab size, CountVectorizer function estimates the term frequency without any approximation involved unlike in HashingTF.
'''

cv = CountVectorizer(inputCol="Words", outputCol="rawFeatures") #minTF=, minDF=2.0
cvModel = cv.fit(tokenized)
featurizedData = cvModel.transform(tokenized)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
print(rescaledData.show())

result = rescaledData.select("*").withColumn("id", monotonically_increasing_id())
result.createOrReplaceTempView('res')
corpus = result.select('id', 'features')

# LDA
lda = LDA(k=10, optimizer='em').setTopicDistributionCol("topicDistributionCol")
model = lda.fit(corpus)
transformed = model.transform(corpus)
transformed.createOrReplaceTempView('transform')
print(transformed.show(truncate=False))

resultsWithTopicDistribution = spark.sql('''SELECT t.topicDistributionCol, r.*
FROM transform as t JOIN res as r ON t.id=r.id AND t.features=r.features
''')
print(resultsWithTopicDistribution.show())
resultsWithTopicDistribution = resultsWithTopicDistribution\
.select(resultsWithTopicDistribution["id"], arrayStringUDF(resultsWithTopicDistribution["topicDistributionCol"]),
 resultsWithTopicDistribution.publishedat, resultsWithTopicDistribution.source)
resultsWithTopicDistribution.write.csv("resultsWithTopicDistribution.csv")
topicTerms = model.describeTopics().withColumn("topics_words", indices_to_terms(cvModel.vocabulary)("termIndices"))
topicTerms = topicTerms\
.select(topicTerms['topic'], zipperUDF(topicTerms['topics_words'], topicTerms['termWeights']).alias('WordsScores'))
# print(topicTerms.rdd.map(zipTermIndicesWordsWeights).collect())
# topicTerms = topicTerms.rdd.map(zipTermIndicesWordsWeights).toDF()
topicTerms.write.csv("topicTerms.csv")
