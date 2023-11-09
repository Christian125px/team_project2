# importing libraries
import pandas as pd
from PIL import Image
import io
import IPython.display as display

# class
class DataIngestor:
    
    def __init__(self):
        """
        Initializes a DataIngestor instance.

        Upon initialization, an instance of the DataIngestor class is created. This instance serves as a utility for loading, saving, and working with data, including data files and images. It provides methods for loading data from files, saving data to files, loading specific columns into lists, and loading and displaying images.

        Usage:
        >>> data_ingestor = DataIngestor()

        After creating an instance, you can use its methods to perform various data-related operations.

        Returns:
        None
        """    
        self.data = None
        print('New instance of DataIngestor initialized')

    def load_file(self, path_to_load, format=None) :
        """
        Load a data file from the specified path and store it as a DataFrame in self.data.
        Supported formats: pickle, csv, xlsx
        """

        if format is None:
            file_extension = path_to_load.split('.')[-1].lower()
            if file_extension == 'pkl':
                self.data = pd.read_pickle(path_to_load)
            elif file_extension == 'csv':
                self.data = pd.read_csv(path_to_load)
            elif file_extension == 'xlsx':
                self.data = pd.read_excel(path_to_load)
            else :
                raise ValueError('Unsupported file format')
        else:
            format = format.lower()
            if format == 'pkl':
                self.data = pd.read_pickle(path_to_load)
            elif format == 'csv':
                self.data = pd.read_csv(path_to_load)
            elif format == 'xlsx':
                self.data = pd.read_excel(path_to_load)
            else :
                raise ValueError('Unsupported file format')

        return self.data
    
    def save_file(self, path_to_save, df):
        """
        Save the DataFrame to a file in the specified path.
        Supported formats: pickle, csv, xlsx
        """

        if df is None:
            raise ValueError("No data to save. You need to load data using load_file method first.")

        file_extension = path_to_save.split('.')[-1].lower()

        if file_extension == 'pkl':
            df.to_pickle(path_to_save)
        elif file_extension == 'csv':
            df.to_csv(path_to_save, index=False)
        elif file_extension == 'xlsx':
            df.to_excel(path_to_save, index=False)
        else:
            raise ValueError('Unsupported file format')
        
    def load_to_list(self, path_to_load, col) :
        """
        Load a specific column of a data file into a list.
        """

        file_extension = path_to_load.split('.')[-1].lower()

        if file_extension == 'pkl':
            data = pd.read_pickle(path_to_load)
        elif file_extension == 'csv':
            data = pd.read_csv(path_to_load)
        elif file_extension == 'xlsx':
            data = pd.read_excel(path_to_load)
        else:
            raise ValueError('Unsupported file format')
        
        if col not in data.columns:
            raise ValueError(f"Column '{col}' not found in the data")
        
        return data[col].tolist()
    
    def out_data(self):
        """
        Return the DataFrame stored in self.data.
        """
        return self.data

    def load_image(self, path_to_image, library='Pillow'):
        """
        Load and display an image file.

        Parameters:
        path_to_image (string): Path to the image file.
        library (string): Library to use for image loading (default is 'Pillow').
        """
        if library == 'Pillow':
            try:
                image = Image.open(path_to_image)
                # image.show() # To avoid the problem of forced conversion to jpeg format(The output will be shown outside the notebook)
                image = image.convert("RGB") # Alternatively you can use the conversion to "RGB" format to obtain the output within the notebook
                display.display(image)
            except Exception as e:
                print(f"Error loading and displaying the image: {e}")
        else:
            raise ValueError("Unsupported image library. Currently, only 'Pillow' is supported.")