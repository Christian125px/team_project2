# importing libraries
import pandas as pd
import numpy as np

# class
class DataFrameCleaner:
    def __init__(self, df):
        """
        Initializes a DataFrameCleaner object.

        Parameters:
        - df (pandas.DataFrame): The DataFrame to be cleaned and manipulated.

        This constructor takes a pandas DataFrame as an argument and creates a copy of it for subsequent operations.
        """
        self.df = df.copy()
    

    def remove_duplicates(self, columns_to_drop_duplicates = []):
        """
        Remove duplicate rows from the DataFrame.

        Parameters:
        - columns_to_drop_duplicates (list, optional): List of column names to use for identifying duplicates.
          If not provided, duplicates are removed based on all columns.

        If columns_to_drop_duplicates is empty, this method removes duplicate rows from the entire DataFrame. 
        If columns_to_drop_duplicates contains column names, the DataFrame is sorted by those columns in descending order, 
        and only the first occurrence of duplicates is retained after sorting.

        Returns:
        None
        """
        if not columns_to_drop_duplicates:
            self.df = self.df.drop_duplicates() # Removing duplicated rows
        else :
            self.df = self.df.sort_values(by=columns_to_drop_duplicates, ascending=False) # Removing rows having duplicates in the specified columns, and keeping only the first row occurrence after sorting
            self.df = self.df.drop_duplicates(subset=columns_to_drop_duplicates, keep='first')
    
    
    def convert_to_nan(self, column, char) :
        """
        Replace specified character with NaN in the specified column.

        Parameters:
        - column (str): Name of the column to operate on.
        - char: The character to be replaced with NaN.

        This method replaces occurrences of the specified character in the specified column with NaN.

        Returns:
        None
        """
        self.df[column] = self.df[column].replace(char, float('nan'))

    
    def remove_char_and_convert_to_float(self, column, char_to_remove, conversion_index, conversion_operator):
        """
        Remove a specific character from a column and convert the values to float.

        Parameters:
        - column (str): Name of the column to operate on.
        - char_to_remove: The character to remove from values.
        - conversion_index: The value to divide or multiply the converted values by.
        - conversion_operator (str): 'division' or 'multiplication' to specify the conversion operator.

        This method removes the specified character from the column's values and performs the specified
        conversion operation (division or multiplication) with the provided conversion index.

        Returns:
        None
        """
        conversion_operator = conversion_operator.lower()
        if conversion_operator == 'division':
            self.df[column] = self.df[column].apply(lambda x: float(x.replace(char_to_remove, ''))/conversion_index if isinstance(x, str) and char_to_remove in x else x)
        elif conversion_operator == 'multiplication':
            self.df[column] = self.df[column].apply(lambda x: float(x.replace(char_to_remove, ''))*conversion_index if isinstance(x, str) and char_to_remove in x else x)
        else:
            print("The required conversion is not available")

    
    def fill_column(self, column, fill_option):
        """
        Fill NaN values in a column with specified statistical values or a custom value.

        Parameters:
        - column (str): Name of the column to operate on.
        - fill_option (str or float): 'mean', 'median', 'mode', or a custom value to fill NaN values.

        This method fills NaN values in the specified column with the mean, median, mode of the column, or a custom value.

        Returns:
        None
        """
        fill_option = fill_option.lower() # Dealing with different formats for fill_option
        if fill_option == 'mean':
            fill_na_value = self.df[column].mean()
        elif fill_option == 'median':
            fill_na_value = self.df[column].median()
        elif fill_option == 'mode':
            fill_na_value = self.df[column].mode().iloc[0]
        else:
            fill_na_value = fill_option
        self.df[column] = self.df[column].fillna(fill_na_value)

    
    def type_conversion(self, column, conversion_type):
        """
        Convert values in a column to a specific data type.

        Parameters:
        - column (str): Name of the column to operate on.
        - conversion_type: The target data type for conversion.

        This method converts the values in the specified column to the provided data type.

        Returns:
        None
        """
        self.df[column] = self.df[column].astype(conversion_type)

    
    def remove_characters(self, column, char_to_remove):
        """
        Remove a specific character from values in a column.

        Parameters:
        - column (str): Name of the column to operate on.
        - char_to_remove: The character to be removed from values.

        This method removes the specified character from values in the specified column.

        Returns:
        None
        """
        self.df[column] = self.df[column].str.replace(char_to_remove, '')

    
    def replace_with_num(self, column, value_to_replace, replacing_value):
        """
        Replace a specific value in a column with a number.

        Parameters:
        - column (str): Name of the column to operate on.
        - value_to_replace: The value to be replaced.
        - replacing_value: The numeric value to replace it with.

        This method replaces occurrences of the specified value with the provided numeric value.

        Returns:
        None
        """
        self.df[column] = self.df[column].replace(value_to_replace, replacing_value)

    
    def replace_with_char(self, column, value_to_replace, replacing_char):
        """
        Replace a specific value in a column with a character.

        Parameters:
        - column (str): Name of the column to operate on.
        - value_to_replace: The value to be replaced.
        - replacing_char: The character to replace it with.

        This method replaces occurrences of the specified value with the provided character.

        Returns:
        None
        """
        self.df[column] = self.df[column].str.replace(value_to_replace, replacing_char)
    
    
    def adjust_dates(self, date_column):
        """
        Convert a column of date strings to datetime objects.

        Parameters:
        - date_column (str): Name of the column containing date strings.

        This method converts date strings in the specified column to datetime objects.

        Returns:
        None
        """
        self.df[date_column] = pd.to_datetime(self.df[date_column])
    
    # Method to rename columns using a dictionary('old_column_name':'new_column_name')
    def rename_columns(self, new_column_names = {}):
        """
        Rename columns in the DataFrame using a dictionary.

        Parameters:
        - new_column_names (dict): A dictionary mapping old column names to new column names.

        This method renames columns in the DataFrame according to the provided dictionary.

        Returns:
        None
        """
        self.df.rename(columns=new_column_names, inplace=True)


    def remove_columns(self, columns_to_remove = []):
        """
        Remove specified columns from the DataFrame.

        Parameters:
        - columns_to_remove (list, optional): List of column names to be removed.

        This method removes the specified columns from the DataFrame.

        Returns:
        None
        """
        self.df = self.df.drop(columns=columns_to_remove)
    
    
    def summary_information(self):
        """
        Display summary information about the DataFrame, including column details and duplicate rows.

        This method prints information about the DataFrame, such as column data types and the number of duplicate rows.

        Returns:
        pandas.DataFrame: The cleaned DataFrame.
        """
        self.df.info()
        print(f'Number of duplicated rows: {sum(self.df.duplicated())}')
        return self.df

    
    def rev_cleaner(self, df_to_clean):
        """
        Clean a DataFrame for "reviews" and return the cleaned DataFrame.

        Parameters:
        - df_to_clean (pandas.DataFrame): The DataFrame containing reviews data.

        This method creates a copy of the input DataFrame and removes duplicate rows and rows with NaN values
        in the 'Translated_Review' column, which is necessary for sentiment analysis.

        Returns:
        pandas.DataFrame: The cleaned DataFrame for reviews.
        """
        cleaned_df = df_to_clean.copy()
        cleaned_df.drop_duplicates(inplace=True) # Dropping duplicates in the df
        cleaned_df = cleaned_df.dropna(subset='Translated_Review') # Dropping rows containing NaN in 'Translated_Review' column since they don't allow us to perform sentiment analysis on the considered app
        return cleaned_df
    
    
    def clean_and_merge(self, first_df, second_df, column_first_df, column_second_df, column_to_drop_na=None):
        """
        Clean a second DataFrame and merge it with the first DataFrame.

        Parameters:
        - first_df (pandas.DataFrame): The first DataFrame to merge with.
        - second_df (pandas.DataFrame): The second DataFrame to be cleaned and merged.
        - column_first_df (str): The column in the first DataFrame to merge on.
        - column_second_df (str): The column in the second DataFrame to merge on.
        - column_to_drop_na (str, optional): The column in the second DataFrame to remove rows with NaN values.
        
        This method cleans the second DataFrame by removing duplicate rows and, if specified, removing rows with NaN values in a specific column.
        Then, it performs an inner merge between the first and cleaned second DataFrames based on the specified columns.

        Returns:
        pandas.DataFrame: The merged DataFrame.
        """
        second_df_clean = second_df.copy()
        second_df_clean.drop_duplicates(inplace=True)
        if column_to_drop_na:
            if column_to_drop_na in second_df_clean.columns:
                second_df_clean = second_df_clean.dropna(subset=[column_to_drop_na])
            else:
                print(f"The column '{column_to_drop_na}' does not exist in the DataFrame.")
        else:
            second_df_clean = second_df_clean.dropna()
        
        # Executing dfs fusion
        merged_df = pd.merge(first_df, second_df_clean, how='inner', left_on=column_first_df, right_on=column_second_df)

        return merged_df

    # Method that returns an example of usage of the DataFrameCleaner class
    def pipeline(self):
        """
        Execute a series of data cleaning and transformation operations on the DataFrame.

        This method performs a series of data cleaning and transformation operations on the DataFrame,
        following a predefined sequence of steps. It includes operations such as filtering rows,
        removing columns, data type conversions, and more. The method also provides a summary of the cleaned DataFrame.

        Returns:
        pandas.DataFrame: The cleaned DataFrame after all operations.
        """
        pd.set_option('display.float_format', lambda x: '%.3f' % x)
        
        self.df = self.df[((self.df['Rating'] >= 1) & (self.df['Rating'] <= 5)) | (self.df['Rating'].isnull())]
        
        columns_to_drop = ['Genres', 'Current Ver', 'Android Ver']
        self.remove_columns(columns_to_drop)
            
        self.remove_duplicates('App')
        self.remove_characters('Size', 'M')
        self.convert_to_nan('Size', 'Varies with device')
        self.remove_char_and_convert_to_float('Size', 'k', 1024, 'division')
        self.type_conversion('Size', float)
        self.fill_column('Rating', 'mean')
        self.fill_column('Size', 'median')
        self.df = self.df.dropna()
        self.type_conversion('Reviews', int)
        self.remove_characters('Installs', ',')
        self.remove_characters('Installs', '+')
        self.type_conversion('Installs', int)
        self.replace_with_num('Type', 'Free', 0)
        self.replace_with_num('Type', 'Paid', 1)
        self.remove_characters('Price', '$')
        self.type_conversion('Price', float)
        self.adjust_dates('Last Updated')
        self.rename_columns({'Content Rating' : 'Content_Rating', 'Last Updated' : 'Last_Updated'})
        
        self.summary_information()
        return self.df