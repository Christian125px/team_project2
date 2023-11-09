from src.data_analyzer import *
from src.database_handler import DatabaseHandler
from src.data_explorator import DataExplorator
from src.dataframe_cleaner import DataFrameCleaner
from src.data_ingestor import DataIngestor
from src.data_visualizer import DataVisualizer


dataing= DataIngestor()
raw_data_path="data/raw_data/"
clean_data_path="data/clean_data/"

####GOOGLE PLAY STORE

df=dataing.load_file(raw_data_path+"googleplaystore.csv")

datacleaner=DataFrameCleaner(df)

df_clean=datacleaner.pipeline()

dataing.save_file(clean_data_path+"gpclean.csv", df_clean)

####GOOGLE PLAY STORE REVIEWS

df_reviews = dataing.load_file(raw_data_path+"googleplaystore_user_reviews.csv")
print('Read review file')

datacleanerrev=DataFrameCleaner(df_reviews)

df_reviews_clean=datacleanerrev.rev_cleaner(df_reviews)

dataing.save_file(clean_data_path+"user_rev_clean.csv", df_reviews_clean)
print("'user_rev_clean.csv' file saved")

####SENTIMENT ANALYSIS

sentiment = DataAnalyzer(df_reviews_clean, p_words, n_words)
df_reviews_sentiment = sentiment.sentiment_score('Translated_Review')
df_reviews_sentiment = sentiment.sentiment_update('Sentiment_Score', 'App', 'App', 'App')
print(df_reviews_sentiment)
dataing.save_file("data/clean_data/sentiment_rev.csv", df_reviews_sentiment)

####CONNECTION TO DATABASE AND INSERTING DATA

db_hand = DatabaseHandler()
db_name = "team_project2"
db_password = "admin"
db_url = "postgresql://postgres:" + db_password + "@localhost:5432/" + db_name
db_hand.create_db(db_url)
db_hand.connect_db(db_url)
db_hand.insert_data(df_clean, 'google_playstore_clean')
db_hand.insert_data(df_reviews_clean, 'google_playstore_user_reviews_clean')
db_hand.insert_data(df_reviews_sentiment, 'google_playstore_user_sentiment')
db_hand.close_connection()

####DATA VISUALIZATION

data_explorator = DataExplorator(df_clean)
data_viz = DataVisualizer(df_clean)

data_explorator.plot_and_save_distribution("Rating", "graphs/")
data_explorator.plot_and_save_distribution("Category", "graphs/")
data_explorator.study_correlations("graphs/heatmap")
data_explorator.study_correlations("graphs/heatmap2", 'Rating,Installs,Reviews')

data_viz.scatter('Price', 'Installs', "graphs/")
data_viz.boxplot("graphs/", 'Installs')
data_viz.boxplot("graphs/", 'Type', 'Installs')