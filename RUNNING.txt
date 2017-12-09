RUNNING.txt
-----------

1] First run the data_acquisition.py script to fetch latest news headlines and metadata and store it to Cassandra table.
Arguments to the program are keyspace and table. Currently database stored on vshukla keyspace.
Use command: python3 data_acquisition.py vshukla newsdata

2] Run newsTopicModels.py which contains the main code for text pre-processing and topic modelling with LDA. It exports two tables in CSV format. 
Use command: spark-submit --packages anguenot:pyspark-cassandra:0.6.0 newsTopicModels.py

3] Run the post-processing script tablesToFeedToTableau.py to make the LDA exported csv files into Tableau data source friendly tables.

4] For Tableau visualisations refer the Tableau Public link https://public.tableau.com/views/TextAnalysisBidDataProject/START?:embed=y&:display_count=yes&publish=yes 

GitLab code repo: https://csil-git1.cs.surrey.sfu.ca/ksanduja/BD-NewsAnalysts.git

