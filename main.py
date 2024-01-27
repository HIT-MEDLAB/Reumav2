import sys
from config import config
from db_engine import DBEngine
from controller import ProcessController


def get_org_tables_data(org_tables_required: list, org_db_engine: DBEngine) -> dict:
    """
    Retrieves original table data based on the provided list of table names.

    Args:
        org_tables_required (list): A list of names of the original tables required.
        org_db_engine (DBEngine): The DBEngine object to interact with the original database.

    Returns:
        dict: A dictionary holding the original table data with table names as keys.
    """
    # Initialize a dictionary to hold the original table data
    org_tables = {}
    for org_table_name in org_tables_required:

        org_table_col_names = org_db_engine.get_columns(org_table_name)

        # Create the right select query if the column Delete_Date exist
        sql_query = f'SELECT * FROM "{org_table_name}"'
        if "Delete_Date" in org_table_col_names:
            sql_query = sql_query + ' WHERE "Delete_Date" IS NULL;'
        else:
            sql_query = sql_query + ';'

        # Add the original table data
        org_tables[org_table_name] = org_db_engine.fetch_data_from_db(sql_query)
    return org_tables


def main():
    # Connection Parameters
    host = config['DB']['HOST']
    port = config['DB']['PORT']
    user = config['DB']['USER']
    password = config['DB']['PASSWORD']
    # Create a connection strings
    dwh_conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/reumav_dwh_staging"
    org_db_conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/testim"

    try:
        # Initialize the Data Warehouse Controller
        dwh_controller = ProcessController(dwh_conn_str)

        # Fetch the data catalog
        data_catalog = dwh_controller.get_data_catalog()
        print("Data catalog received")

        # Get the names of the original tables required for this run
        org_tables_required = data_catalog['table_name'].unique()

        # Initialize the Original Database Engine and fetch original tables data
        org_db_engine = DBEngine(org_db_conn_str)
        org_tables = get_org_tables_data(org_tables_required, org_db_engine)
        org_db_engine.dispose()
        print("Original tables received")

        # Uncomment this if you want to update encounters table
        # dwh_controller.update_encounters_table(org_tables)

        # Process and transform original tables into Data Warehouse
        dwh_controller.process_org_tables_into_dwh(org_tables)

        sys.exit()
    except Exception as e:
        raise e


if __name__ == "__main__":
    main()
