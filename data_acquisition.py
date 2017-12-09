from newsapi.articles import Articles
# import pymongo
# from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
import json
import pprint
import sys

# Key to make requests through API
api = Articles(API_KEY="3e40232f1ea246cb85c76b46bc7543d3")

# The sources selected
sources = ["abc-news-au","al-jazeera-english","ars-technica","associated-press","bbc-news","bbc-sport","bild","bloomberg","breitbart-news","business-insider",
"business-insider-uk","buzzfeed","cnbc","cnn","daily-mail","engadget","entertainment-weekly","espn",
"espn-cric-info","financial-times","focus","football-italia","fortune","four-four-two","fox-sports","google-news","gruenderszene",
"hacker-news","ign","independent","mashable","metro","mirror","mtv-news","mtv-news-uk","national-geographic","new-scientist",
"newsweek","new-york-magazine","nfl-news","polygon","recode","reddit-r-all","reuters","spiegel-online","t3n","talksport","techcrunch",
"techradar","the-economist","the-guardian-au","the-guardian-uk","the-hindu","the-huffington-post","the-lad-bible","the-new-york-times",
"the-sport-bible","the-telegraph","the-times-of-india","the-verge","the-wall-street-journal","the-washington-post",
"time","usa-today","wired-de"]
'''["the-wall-street-journal", "al-jazeera-english", "bbc-news", "bloomberg", "business-insider", "cnbc", "cnn", "daily-mail", 
	"engadget", "espn", "financial-times", "fortune", "fox-sports", "mtv-news", "new-scientist","new-york-magazine","nfl-news","reuters",
	 "talksport","techcrunch","the-economist","the-guardian-uk","the-hindu","the-new-york-times","the-sport-bible","the-times-of-india",
	  "the-verge","the-wall-street-journal","time","usa-today"]'''

#wsj = api.get(source="the-wall-street-journal", sort_by="top")

# wsj_arts_json = json.dumps(wsj.articles,ensure_ascii=False)
# type(wsj_arts_json)

def main(keyspace, table):
	cluster = Cluster(['199.60.17.171', '199.60.17.188'])
	session = cluster.connect(keyspace)

	insert_statement = SimpleStatement("INSERT INTO " +table+ \
		" (author, description, publishedAt, title, url, urlToImage, source) VALUES (%s, %s, %s, %s, %s, %s, %s)")

	count = 0
	batch = BatchStatement()

	for i in sources:
		# api.get(source=i, sort_by="popular")	
		# print("News Source: " + str(i))
		fetched=api.get_by_top(source=i)
		# jsonobj = json.dumps(fetched)
		fetcheddictobj = dict(fetched)
		source = fetcheddictobj['source']
		for art in fetcheddictobj['articles']:
			author = art['author']
			description = art['description']
			publishedAt = art['publishedAt']
			title = art['title']
			url = art['url']
			urlToImage = art['urlToImage']
			batch.add(insert_statement,(author, description, publishedAt, title, url, urlToImage, source))
			count = count + 1
			if count == 50:
				count = 0
				session.execute(batch)
				print('Batch of 50 insert statements executed')
				batch.clear()
	session.execute(batch)

if __name__ == '__main__':
	# input_dir = sys.argv[1]
	keyspace = sys.argv[1]
	table = sys.argv[2]
	main(keyspace, table)
