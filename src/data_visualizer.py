# importing libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from src.data_explorator import DataExplorator # if you want to use this class within this file remove "src." before "data_explorator"

# class
class DataVisualizer(DataExplorator):
    def __init__(self,data):
        """
        Initializes a DataVisualizer instance for data visualization.

        Parameters:
        - data (pandas.DataFrame): The DataFrame containing data for visualization.

        This constructor creates an instance of DataVisualizer, which is designed for data visualization. It inherits functionality from the DataExplorator class and allows you to create scatter plots and box plots for data exploration and visualization.

        Usage:
        >>> data_visualizer = DataVisualizer(data)

        Returns:
        None
        """
        super().__init__(data)
    
    def scatter(self, x, y, path):
        """
        Create a scatter plot to visualize the relationship between two numeric variables.

        Parameters:
        - x (str): Name of the column for the x-axis.
        - y (str): Name of the column for the y-axis.

        This method generates a scatter plot to visualize the relationship between two numeric variables in the DataFrame. It checks if both specified columns exist in the DataFrame and whether they are of numeric data types. If the conditions are met, a scatter plot is created with the specified variables on the x and y axes.

        Raises:
        - ValueError: If either x or y is not a numeric column or if they are not present in the DataFrame.

        Returns:
        None
        """
        if x in self.data.columns and y in self.data.columns: 
            if pd.api.types.is_numeric_dtype(self.data[x]) and pd.api.types.is_numeric_dtype(self.data[y]):
                plt.figure()
                plt.scatter(self.data[x], self.data[y],alpha=0.5) 
                plt.xlabel(f"{x}")
                plt.ylabel(f"{y}")
                plt.title(f"{x}_{y} scatter")
                plt.savefig(path + x + '_' + y + '_scatter')
            else:
                raise ValueError("both variables must be numeric type data")
        else:
            raise ValueError("x and y must be columns of the dataframe")
        
    def boxplot(self, path, x,y=""):
        """
        Create a box plot to visualize the distribution of data.

        Parameters:
        - x (str): Name of the column for the x-axis.
        - y (str, optional): Name of the column for grouping data (for grouped box plots).

        This method generates a box plot to visualize the distribution of data in the DataFrame. It can create a simple box plot for a single variable (x) or a grouped box plot when both x and y are specified. The size of the plot can be adjusted for clarity.

        Raises:
        - ValueError: If the specified column(s) do not exist in the DataFrame or if there is an issue with the data.

        Returns:
        None
        """
        plt.figure(figsize=(7,5))
        if y!= "":
            plt.figure()
            sns.boxplot(x=x,y=y, data=self.data)
            plt.title(f"{x}_{y} boxplot")
            plt.savefig(path + x + '_' + y + '_boxplot')
        else:
            plt.figure()
            sns.boxplot(y=x, data=self.data)
            plt.title(f"{x} boxplot")
            plt.savefig(path + x + '_boxplot')

# usage example            
if __name__ == '__main__':
    df = pd.read_csv("data/raw_data/gp_clean.csv")
    test=DataVisualizer(df)
    test.scatter("Rating","Installs", 'graphs/')