from typing import List, Union
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table, insert


class DBEngine:
    """
    A class to manage database engine connections and to inspect database tables.

    Attributes:
        engine: The SQLAlchemy engine instance for database connection.
        inspector: The SQLAlchemy inspector instance for database inspection.
        table_inspector_columns (dict): A cache dictionary to store inspector columns of a table.

    Methods:
        get_columns(self, table_name: str) -> List[str]
        get_not_null_columns(self, table_name: str) -> List[str]
        dispose(self) -> None
        fetch_data_from_db(self, sql_query: str) -> pd.DataFrame
        validate_and_save_new_row(self, new_row: dict, dwh_table_name: str) -> None
        save_new_row_to_dwh(self, new_row: dict, dwh_table_name: str) -> None
        def validate_row(self, new_row: dict, dwh_table_name: str) -> bool
    """

    def __init__(self, connection_string: str) -> None:
        """
        Initializes a new instance of the DBEngine class.

        Args:
            connection_string (str): A connection string to connect to the database.
        """
        self.engine = create_engine(connection_string, encoding='utf8')
        self.inspector = inspect(self.engine)
        self.table_inspector_columns = {}

    def get_columns(self, table_name: str) -> List[str]:
        """
        Retrieves column names of the specified table. This method checks the cache dictionary 'table_inspector_columns'
        first. If the inspector of the specified table not found in the cache, it inspects the database to get the right
        inspector, stores in the cache, and returning its column names.

        Args:
            table_name (str): The name of the table whose columns are to be retrieved.

        Returns:
            List[str]: A list containing the names of the columns of the specified table.
        """
        if table_name not in self.table_inspector_columns:
            self.table_inspector_columns[table_name] = self.inspector.get_columns(table_name)
        return [col['name'] for col in self.table_inspector_columns[table_name]]

    def get_not_null_columns(self, table_name: str) -> List[str]:
        """
        Retrieves names of the NOT NULL columns of the specified table.

        Args:
            table_name (str): The name of the table whose NOT NULL columns are to be retrieved.

        Returns:
            list: A list containing the names of the NOT NULL columns of the specified table.
        """
        if table_name not in self.table_inspector_columns:
            self.table_inspector_columns[table_name] = self.inspector.get_columns(table_name)

        not_null_columns = []
        for col in self.table_inspector_columns[table_name]:
            if not col['nullable']:
                not_null_columns.append(col['name'])
        return not_null_columns

    def dispose(self) -> None:
        """
        Disposes of the connection engine.
        It's essential to call this method to release the database connection resources
        when they are no longer needed.
        """
        self.engine.dispose()

    def fetch_data_from_db(self, sql_query: str) -> pd.DataFrame:
        """
        Fetches data from the database and loads it into a DataFrame.

        Args:
            sql_query (str): The SQL query string used to fetch the data.

        Returns:
            pd.DataFrame: A DataFrame containing the fetched data, with NaN replaced by None.
        """
        # Use pandas to execute the SQL query and fetch data into a DataFrame
        df = pd.read_sql_query(sql_query, self.engine)

        # Replace NaN values with None (as was done in the original code)
        df = df.astype(object).replace(np.nan, None)
        return df

    def save_new_row_to_dwh(self, new_row: Union[dict, List[dict], pd.DataFrame], dwh_table_name: str) -> None:
        """
        Save a new row to the specified data warehouse table. This function prepares the insert statement and then
        executes it to insert the new row into the designated table in the data warehouse.

        Args:
            new_row (dict|List[dict]|pd.DataFrame): New row\rows to be added to the data warehouse table.
            dwh_table_name (str): The name of the target data warehouse table.
        """
        if isinstance(new_row, pd.DataFrame):
            # Change dataframe to dictionary
            new_row = new_row.to_dict(orient='records')

        engine = self.engine
        # Create metadata object
        metadata = MetaData()
        # Reflect the table
        table = Table(dwh_table_name, metadata, autoload_with=engine)
        # Prepare the insert statement
        stmt = insert(table).values(new_row)
        # Execute the statement
        with engine.connect() as connection:
            connection.execute(stmt)



