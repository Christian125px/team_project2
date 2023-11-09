from src.data_analyzer import *
from src.database_handler import DatabaseHandler
from src.data_explorator import DataExplorator
from src.dataframe_cleaner import DataFrameCleaner
from src.data_ingestor import DataIngestor
from src.data_visualizer import DataVisualizer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import os
import json

default_dag_args={
    "start_date": datetime(2023,9,20),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries":1,
    "retry_delay": timedelta(minutes=5),
}

dataing=DataIngestor()
raw_data_path="/Users/gian/Desktop/data/raw_data/"
clean_data_path="/Users/gian/Desktop/data/clean_data/"

def googleplaystore():
    df=dataing.load_file(raw_data_path+"googleplaystore.csv")
    datacleaner=DataFrameCleaner(df)
    df_clean=datacleaner.pipeline()
    dataing.save_file(clean_data_path+"gpclean.csv", df_clean)

def google_user_reviews():
    df_reviews = dataing.load_file(raw_data_path+"googleplaystore_user_reviews.csv")
    datacleanerrev=DataFrameCleaner(df_reviews)
    df_reviews_clean=datacleanerrev.rev_cleaner(df_reviews)
    dataing.save_file(clean_data_path+"user_rev_clean.csv", df_reviews_clean)

def sentiment_analysis():
    df_reviews_clean=dataing.load_file(clean_data_path+"user_rev_clean.csv")
    sentiment = DataAnalyzer(df_reviews_clean, p_words, n_words)
    df_reviews_sentiment = sentiment.sentiment_score('Translated_Review')
    df_reviews_sentiment = sentiment.sentiment_update('Sentiment_Score', 'App', 'App', 'App')
    dataing.save_file(clean_data_path+"sentiment_rev.csv", df_reviews_sentiment)

def db_handler():
    db_hand = DatabaseHandler()
    db_name = "team_project2"
    db_password = "admin"
    db_url = "postgresql://postgres:" + db_password + "@localhost:5432/" + db_name
    db_hand.create_db(db_url)
    db_hand.connect_db(db_url)
    df_clean=dataing.load_file(clean_data_path+"gpclean.csv")
    db_hand.insert_data(df_clean, 'google_playstore_clean')
    df_reviews_clean=dataing.load_file(clean_data_path+"user_rev_clean.csv")
    db_hand.insert_data(df_reviews_clean, 'google_playstore_user_reviews_clean')
    df_reviews_sentiment=dataing.load_file(clean_data_path+"sentiment_rev.csv")
    db_hand.insert_data(df_reviews_sentiment,"gplaystore_user_sentiment")
    db_hand.close_connection()





with DAG("Team_Project2", schedule_interval=None, default_args=default_dag_args) as dag_python:
    t0=PythonOperator(task_id="google_play_store", python_callable=googleplaystore)
    t1=PythonOperator(task_id="google_user_reviews", python_callable=google_user_reviews)
    t2=PythonOperator(task_id="sentiment_analysis", python_callable=sentiment_analysis)
    t3=PythonOperator(task_id="db_handler", python_callable=db_handler)

    t0>>t1>>t2>>t3