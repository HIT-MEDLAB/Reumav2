import tkinter as tk
from tkinter import filedialog
import numpy as np
import pandas as pd
import psycopg2


def get_excel_file_path() -> str:
    """
    Prompt the user to select an Excel file and return its path.

    Returns:
        str: The path to the selected Excel file.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    return filedialog.askopenfilename(title="Select an Excel file", filetypes=[("Excel files", "*.xlsx")])


def preprocess_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Preprocess the DataFrame based on the table name.

    Args:
        df (pd.DataFrame): The DataFrame to preprocess.
        table_name (str): The name of the table for which the DataFrame is being preprocessed.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """
    # Remove unnecessary columns
    if table_name in ['Demographics', 'Disease_Activity', 'BMD_Assessment', 'Imaging', 'Functional_Tests']:
        columns_to_drop = ["Axial SpA_Heading03", "Id_Num"]
        if table_name in ['BMD_Assessment', 'Imaging', 'Functional_Tests']:
            columns_to_drop.append("Date")
        for col in columns_to_drop:
            if col in df.columns:
                df.drop(columns=col, inplace=True)
                print(f"\033[47m \033[93m Column '{col}' was dropped from the DataFrame.\033[0m")

    # Rename columns if needed
    if 'UnitName' in df.columns:
        df.rename(columns={'UnitName': 'Unit_Name'}, inplace=True)
        print('\033[47m \033[93m Column "UnitName" changed to "Unit_Name"\033[0m')

    # Remove duplicates based on 'Namer_Steel_No' in Demographics
    if table_name == 'Demographics':
        duplicated_rows = df[df['Namer_Steel_No'].duplicated(keep=False)]
        # If there are any duplicated rows, print them
        if not duplicated_rows.empty:
            print("\033[47m \033[93m Duplicated rows:\033[0m")
            print(duplicated_rows)
            # Drop duplicates, keeping only the first occurrence
            df.drop_duplicates(subset='Namer_Steel_No', keep='first', inplace=True)
            print("\033[47m \033[93m Removed duplications\033[0m")

    # The NULL imported like NaN so here we change the NaN to None
    df = df.astype(object).replace(np.nan, None)
    print('\033[92m Changed NaN to None\033[0m')

    # Check for invalid addiction statuses
    addiction_status_columns = ["Smoking_Status", "Alcohol_Status"]
    for col in addiction_status_columns:
        if col in df.columns and (df[col] == 101).any():
            df.loc[df[col] == 101, col] = 0
            print(f"\033[47m \033[93m Column '{col}' had the value 101 and it was changed to 0.\033[0m")

    # Change non-numeric values in some columns to None
    col_to_check = []
    if table_name == 'Disease_Activity':
        col_to_check = ['PASI_121', 'ASDAS_23', 'Duration MS (0-10)_43', 'DAS-28_22']
    elif table_name == 'BMD_Assessment':
        col_to_check = ['TSCORE_L2_L4', 'TSCORE_HIP_R', 'TSCORE_HIP_L']
    for col in col_to_check:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x if isinstance(x, (float, int)) else None)

    return df


def insert_data_into_db(cur, df: pd.DataFrame, table_name: str) -> None:
    """
    Insert data from the DataFrame into the specified table in the database.

    Args:
        cur (psycopg2.extensions.cursor): The database cursor to use for insertion.
        df (pd.DataFrame): The DataFrame containing data to be inserted.
        table_name (str): The name of the table in the database where data should be inserted.
    """
    try:
        for index, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns_list = [str(item).strip() for item in row.keys()]
            columns = '", "'.join(columns_list)
            sql = f'INSERT INTO "{table_name}" ("{columns}") VALUES ({placeholders})'
            cur.execute(sql, tuple(row))
    except Exception as e:
        paired_data = list(zip(columns_list, tuple(row)))
        # Create a table header
        table_header = "\033[01m\033[04mColumn\033[0m\033[41m:\t\t\t\033[01m\033[04mValue\033[0m\033[41m"
        table_rows = [f"{col}:\t{val}" for col, val in paired_data]
        table = '\n'.join([table_header] + table_rows)
        print(f"\033[41m "
              f"\033[01m\033[04mError encountered while inserting into table:\033[0m\033[41m"
              f" {table_name}\n"
              f"\033[01m\033[04m Row index:\033[0m\033[41m"
              f" {index}\n"
              f"\033[01m\033[04m Values:\033[0m\033[41m\n"
              f"{table}\n"
              f"\033[01m\033[04m Error message:\033[0m\033[41m"
              f" {e}")
        raise  # This will re-raise the caught exception after printing the error messages


def main():
    file_path = get_excel_file_path()
    print('File path:', file_path)

    # Load the Excel file
    xls = pd.ExcelFile(file_path, engine='openpyxl')
    sheet_names = xls.sheet_names[::-1]
    print('File sheets names:', sheet_names)

    # Database connection parameters
    db_params = {
        'dbname': 'testim',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5432'
    }
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Loop through each sheet in the Excel file
    for sheet_name in sheet_names:
        print(f"\033[94m === Working on {sheet_name} ==== \033[0m")
        # Read the data from the current sheet into a DataFrame
        df = pd.read_excel(xls, sheet_name, engine='openpyxl')
        table_name = sheet_name[1:]
        df = preprocess_dataframe(df, table_name)
        insert_data_into_db(cur, df, table_name)
    conn.commit()
    cur.close()
    conn.close()
    print("Data imported successfully!")


if __name__ == "__main__":
    main()
