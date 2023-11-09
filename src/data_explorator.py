# importing libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# class
class DataExplorator:
    def __init__(self, data):
        """
        Initializes a DataExplorator object with a dataset.

        Parameters:
        - data (pandas.DataFrame): The dataset to explore and analyze.

        This constructor takes a pandas DataFrame as an argument and initializes the DataExplorator object.
        The object will be used to perform various exploratory data analysis tasks on the provided dataset.
        """
        self.data = data
    
    
    def describe_variable(self, variable_name):
        """
        Describe a specific variable in the dataset and visualize its distribution.

        Parameters:
        - variable_name (str): The name of the variable to describe and plot.

        This method provides descriptive statistics and plots the distribution of the specified variable.
        For numeric variables, it displays statistics such as mean, standard deviation, and skewness and plots a histogram with a KDE.
        For categorical variables, it shows the value counts and creates a bar graph (countplot).

        Raises:
        - ValueError: If the specified variable does not exist in the dataset.

        Returns:
        None
        """
        # Check whether the specified variable exists in the dataset
        if variable_name not in self.data.columns:
            raise ValueError(f"The variable '{variable_name}' does not exist in the dataset.")
        
        # Series (column) representing the data contained in the column of the DataFrame with the name specified in variable_name
        variable = self.data[variable_name] 
        
        if pd.api.types.is_numeric_dtype(variable):
            # Calculate descriptive statistics
            description = variable.describe()
            
            # Calculate the asymmetry
            skewness = variable.skew()
            
            # Print statistics
            print(f"Descriptive statistics for the variable '{variable_name}':")
            print(description)
            print(f"Asymmetry: {skewness}")
        else:
            count = variable.value_counts()

            # Print statistics
            print(f"Descriptive statistics for the variable '{variable_name}':")
            print(f"Count: {count}")
        
        # Create the distribution graph
        self.plot_distribution(variable_name)
    
    
    def plot_and_save_distribution(self, variable_name, path):
        """
        Plot the distribution of a specific variable in the dataset.

        Parameters:
        - variable_name (str): The name of the variable to plot.

        This method generates a distribution plot for the specified variable. The type of plot (histogram or countplot) is determined by the variable's data type (numeric or categorical).

        Returns:
        None
        """
        variable = self.data[variable_name]
        # Check the type of variable (numeric or categorical)
        if pd.api.types.is_numeric_dtype(variable):
            # If it is a numeric variable, create a histogram with a kernel density estimate(KDE) plot
            plt.figure(figsize=(8, 6))
            sns.histplot(variable, kde=True)
            plt.title(f'Distribution of {variable_name}')
            plt.xlabel(variable_name)
            plt.ylabel('Frequency')
            plt.savefig(path + variable_name + '_plot')
        else:
            # If it is a categorical variable, create a bar graph(countplot)
            plt.figure(figsize=(8, 6))
            sns.countplot(y=variable, data=self.data, order = variable.value_counts().index)
            plt.title(f'Distribution of {variable_name}')
            plt.xlabel('Count')
            plt.savefig(path + variable_name + '_plot')
    
    
    def study_correlations(self, path, columns_cor=[], cmap=""):
        """
        Visualize correlations between variables in the dataset using a heatmap.

        Parameters:
        - columns_cor (str, optional): A comma-separated string of column names to study correlations. If empty, correlations for all numeric columns are computed and visualized.
        - cmap (str, optional): The colormap to use for the heatmap. Default is "coolwarm."

        This method creates a heatmap to visualize correlations between the specified columns or all numeric columns in the dataset. The heatmap provides a visual representation of the correlation coefficients, with color intensity indicating the strength and direction of the correlation.

        Returns:
        None
        """
        if not cmap: #check on the color
            cmap="coolwarm"

        if not columns_cor:
            plt.figure()
            sns.heatmap(self.data.select_dtypes(include=['number']).corr(), annot=True, cmap=cmap, center=0)
            plt.title('Overall Heatmap')
            plt.savefig(path)
        else:
            columns_cor= columns_cor.split(",")
            plt.figure()
            sns.heatmap(self.data[columns_cor].corr(), annot=True, cmap=cmap, center=0)
            plt.title(f'Heatmap of: {columns_cor}')
            plt.savefig(path)