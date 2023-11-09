# importing libraries
import pandas as pd
import numpy as np
from afinn import Afinn

# class

class DataAnalyzer:
    def __init__(self, dataframe, positive_words, negative_words):
        """
        Initializes a DataAnalyzer instance.

        Parameters:
        - dataframe (pandas.DataFrame): The DataFrame containing data to be analyzed.
        - positive_words (list): A list of positive sentiment words used for sentiment scoring.
        - negative_words (list): A list of negative sentiment words used for sentiment scoring.

        Upon initialization, a DataAnalyzer instance is created. This instance is designed to analyze sentiment in text data within a given DataFrame using the AFINN lexicon. It requires lists of positive and negative words for sentiment analysis. The AFINN lexicon is preprocessed to assign sentiment scores to these words.

        Usage:
        >>> data_analyzer = DataAnalyzer(dataframe, positive_words, negative_words)

        The AFINN lexicon assigns a score of 2 to positive words and -2 to negative words. This modified lexicon is used for sentiment scoring in the text data.

        Returns:
        None
        """
        self.dataframe = dataframe
        self.positive_words = positive_words
        self.negative_words = negative_words
        self.afn = Afinn()
        
        positive_words_scores = {word: 2 for word in self.positive_words}
        negative_words_scores = {word: -2 for word in self.negative_words}

        for word, score in positive_words_scores.items():
            self.afn._dict[word] = score

        for word, score in negative_words_scores.items():
            self.afn._dict[word] = score

    def sentiment_score(self, column):
        """
        Calculate sentiment scores for the specified column and add 'Sentiment_Score' to the dataframe.

        Args:
            column (str): Name of the column containing text reviews.

        Returns:
            None
        """
        if column not in self.dataframe.columns:
            raise ValueError(f"The variable '{column}' does not exist in the dataset")
        
        self.dataframe['Sentiment_Score'] = [self.afn.score(review) for review in self.dataframe[column]]

    def sentiment_update(self, column, left_on, right_on, group_on=[]):
        """
        Add average sentiment scores to the dataframe based on specified columns.

        Args:
            column (str): Name of the column with sentiment scores.
            left_on (str): Name of the left dataframe column for merging.
            right_on (str): Name of the right dataframe column for merging.
            group_on (list): List of columns to group by for calculating average sentiment.

        Returns:
            pd.DataFrame: Updated dataframe with 'Average_Score' column added.
        """
        if column not in self.dataframe.columns:
            raise ValueError(f"The variable '{column}' does not exist in the dataset")
        
        if not group_on:
            raise ValueError("You have to define grouping column/s")

        average_sentiment_df = self.dataframe.groupby(group_on)[column].mean()
        self.dataframe = self.dataframe.merge(average_sentiment_df, left_on=left_on, right_on=right_on, how='inner')
        self.dataframe.rename(columns={'Sentiment_Score_x': 'Sentiment_Score', 'Sentiment_Score_y': 'Average_Score'}, inplace=True)

        return self.dataframe
    
p_words = pd.read_excel("data/raw_data/p.xlsx")
n_words = pd.read_excel("data/raw_data/n.xlsx")