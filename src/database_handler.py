# example URL(postgres): postgresql://username:password@host:port/database_name (postgresql://postgres:postgres@localhost:5432/mydatabase)
import pandas as pd
import json
import sqlalchemy as sql
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

class DatabaseHandler:

    def __init__(self):
        """
        Initializes a new instance of the DatabaseHandler class.
        This class is designed to handle database operations, such as
        creating databases, connecting to them, inserting data, and executing queries.
        """
        self.engine = None
        print("Initialized class")
  
    def create_db(self, url):
        """
        Creates a new database if it doesn't already exist at the provided URL.

        Args:
            url (str): The database URL.

        """
        self.engine = create_engine(url)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            print("Database created")
        else:
            print("Database already exists")

    def connect_db(self, url):
        """
        Connects to an existing database at the provided URL.

        Args:
            url (str): The database URL.

        """
        self.engine = create_engine(url)
        if database_exists(self.engine.url):
            self.engine.connect(url)
            print("Database connected")
        else:
            print("Database doesn't exist")
    
    def insert_data(self, data, tablename):
        """
        Inserts data from a JSON or CSV file into a specified table in the database.

        Args:
            data (str): The path to the data file (JSON or CSV).
            tablename (str): The name of the table to insert the data into.

        Raises:
            ValueError: If the file format is not supported.

        """
        if isinstance(data, pd.DataFrame):
            df = data
        elif data.endswith(".json"):
            with open(data, 'r') as f:
                file = json.load(f)
            df = pd.DataFrame(file)
        elif data.endswith(".csv"):
            df = pd.read_csv(data)
        else:
            raise ValueError("Unsupported file format")
        df.to_sql(tablename, con=self.engine, if_exists='replace', index=False)
        print("Data entered correctly")

    def executor(self, query):
        """
        Executes a SQL query on the connected database.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of rows as query results.

        """
        try:
            result = self.engine.execute(query)
            rows = [row for row in result]
            return rows
        except Exception as e:
            print(f"Error: '{e}'")

    def close_connection(self):
        """
        Closes the connection to the database.

        """
        self.engine.dispose()
        print("Database connection closed")

# usage example
if __name__ == '__main__':
    test=DatabaseHandler()

    test.connect_db("postgresql://postgres:postgres@localhost:5432/team_project2")
    df = pd.read_csv("clean_data/gpclean.csv")
    test.insert_data(df, "gpclean3")

    #print(test.executor("SELECT * FROM public.revclean LIMIT 3"))

