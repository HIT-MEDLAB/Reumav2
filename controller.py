import json
import re
from datetime import datetime
from typing import Union
import pandas as pd
from db_engine import DBEngine
from errors import MandatoryFieldMissing, NotNullColumnIsMissing, OriginalDataFieldIsMissing
from log_printer import LogPrinter, write_to_log_file
from translator import translate


class ProcessController:
    """
    A class for processing original tables and inserting them into the data warehouse.

    Attributes:
        dwh_engine (DBEngine): The Data Warehouse Engine instance.
        dwh_dictionary (pd.DataFrame): A DataFrame to store translations from Hebrew to English.
        data_catalog (pd.DataFrame): A DataFrame containing relevant rows from the data catalog.
        encounters (pd.DataFrame): A DataFrame containing encounters table.
        current_org_table_name (str): The name of the current original table being processed.
        current_dwh_table_name (str): The name of the current data warehouse table being processed.
        batch_que (dict): The que of all the rows that need to be saved in the data warehouse

    Methods:
        get_data_catalog(self) -> pd.DataFrame:
            Retrieves and sorts the relevant rows from the data_catalog.

        process_org_tables_into_dwh(self, org_tables: dict) -> None:
            Processes original tables and inserts them into the data warehouse.

        process_org_row_into_dwh(self, org_row: pd.Series, org_table_name: str) -> None:
            Processes a row from the original table and inserts it into the appropriate data warehouse tables.

        process_row_by_rules(self, org_row: pd.Series, org_table_name: str, basic_cols: dict,
                             dwh_table_name: str, cat_rows: pd.DataFrame = pd.DataFrame()) -> None:
            Processes rows by applying specific rules and saves them into the designated data warehouse table.

        translate_heb_words(self, row: dict) -> dict:
            Searches for words in Hebrew in the row and translates them to English.
    """
    dwh_engine: DBEngine = None
    dwh_dictionary: pd.DataFrame = None
    data_catalog: pd.DataFrame = None
    encounters: pd.DataFrame = None
    current_org_table_name: str = ''
    current_dwh_table_name: str = ''
    batch_que: dict = {}
    logger: LogPrinter = None

    def __init__(self, dwh_conn_str: str) -> None:
        """
        Initializes a new instance of the ProcessController class.

        Args:
            dwh_conn_str (str): The connection string for the Data Warehouse.
        """
        # Initialize the Data Warehouse Engine
        self.dwh_engine = DBEngine(dwh_conn_str)

    def get_data_catalog(self) -> pd.DataFrame:
        """
        Retrieves and sorts the relevant rows from the data_catalog.

        Returns:
            pd.DataFrame: A sorted DataFrame containing the relevant rows from the data_catalog.
        """
        # Get the relevant rows from the data_catalog
        sql_query = "SELECT * FROM data_catalog WHERE sw_ignore = 0 AND stand_by = 0 AND target_table IS NOT NULL AND target_column IS NOT NULL;"
        # Fetch data and sort by 'target_table'
        self.data_catalog = self.dwh_engine.fetch_data_from_db(sql_query)
        self.data_catalog.sort_values(by='target_table', inplace=True)
        return self.data_catalog

    def process_org_tables_into_dwh(self, org_tables: dict) -> None:
        """
        Processes original tables and inserts them into the data warehouse.

        Args:
            org_tables (dict): A dictionary containing the original tables.
        """
        # Create log printer with the total original data roes count
        # and record the start time
        self.logger = logger = LogPrinter(sum(len(table) for table in org_tables.values()))
        logger.tables_process_started()
        # Loop through each original table
        for org_table_name, org_table in org_tables.items():
            self.current_org_table_name = org_table_name
            logger.specific_table_process_started(org_table_name, len(org_table))
            # Loop through each original data row
            for i, org_row in org_table.iterrows():
                self.process_org_row_into_dwh(org_row)
                logger.row_processed()
            logger.specific_table_process_ended(org_table_name)
        self.save_all_rows_in_que()
        logger.tables_process_ended()
        self.dwh_engine.dispose()

    def process_org_row_into_dwh(self, org_row: pd.Series) -> None:
        """
        Process a row from the original table and insert it into the appropriate data warehouse tables. If information
        is missing in the original data, log error messages and continue to the next data warehouse table.

        Args:
            org_row (pd.Series): The row from the original table to be processed.
        """
        data_catalog = self.data_catalog
        org_table_name = self.current_org_table_name
        # Get the relevant rows for the current table from the data catalog
        org_table_catalog = data_catalog[data_catalog['table_name'] == org_table_name]
        # Loop through each data warehouse table
        for dwh_table_name in org_table_catalog['target_table'].unique():
            self.current_dwh_table_name = dwh_table_name
            # Get only the rows matching current original table and current dwh table
            org_dwh_tbl_cat = org_table_catalog[org_table_catalog['target_table'] == dwh_table_name]
            try:
                mandatory_cols = self.get_mandatory_cols(org_dwh_tbl_cat, org_row)
            except MandatoryFieldMissing as e:
                self.handle_exception(org_row, {}, e)
                # Exit the dwh_table_name loop, and continue to the next dwh_table_name if information is missing
                continue

            if dwh_table_name == 'concept_dimension':
                self.concept_dimension_process(mandatory_cols, org_row)
            elif dwh_table_name == 'patient_dimension':
                self.patient_dimension_process(mandatory_cols, org_row, org_dwh_tbl_cat)
            elif dwh_table_name == 'observation_fact':
                self.observation_fact_process(mandatory_cols, org_row, org_dwh_tbl_cat)

    def concept_dimension_process(self, mandatory_cols: dict, org_row: pd.Series) -> None:
        """
        Processes rows to concept_dimension table.

        Args:
            mandatory_cols (dict): A dictionary holding mandatory columns values for the row.
            org_row (pd.Series): The row from the original table.
        """
        # Fill the 'name_char'
        if 'concept_desc' in mandatory_cols:
            mandatory_cols['name_char'] = mandatory_cols['concept_desc']
        # Check for words in hebrew and translate them
        mandatory_cols = self.translate_heb_words(mandatory_cols)
        try:
            self.validate_row(mandatory_cols)
            self.add_row_to_batch(mandatory_cols, self.current_dwh_table_name)
        except NotNullColumnIsMissing as e:
            self.handle_exception(org_row, mandatory_cols, e)

    def patient_dimension_process(self, mandatory_cols: dict, org_row: pd.Series,
                                  org_dwh_tbl_cat: pd.DataFrame) -> None:
        """
        Processes rows to patient_dimension table, by applying specific rules from the data catalog. Each row is
        processed by taking values from the original row and applying them to the new row, which is then saved to the data
        warehouse table.

        Args:
            mandatory_cols (dict): A dictionary holding mandatory columns values for the row.
            org_row (pd.Series): The row from the original table.
            org_dwh_tbl_cat (pd.DataFrame): The relevant data catalog part for this original and data warehouse table.
        """
        # Add fields based on catalog rows with 'modifier_cd' == '@', don't exit if the value not exist.
        no_modifier_rows = org_dwh_tbl_cat[org_dwh_tbl_cat['modifier_cd'] == '@']
        for _, catalog_row in no_modifier_rows.iterrows():
            org_col_name = catalog_row['column_name']
            if pd.notna(org_row[org_col_name]):
                # For patient_dimension we merge the catalog rules to one row.
                mandatory_cols[catalog_row['target_column']] = org_row[catalog_row['column_name']]
        # Check for words in hebrew and translate them
        mandatory_cols = self.translate_heb_words(mandatory_cols)
        try:
            self.validate_row(mandatory_cols)
            self.add_row_to_batch(mandatory_cols, self.current_dwh_table_name)
        except NotNullColumnIsMissing as e:
            self.handle_exception(org_row, mandatory_cols, e)

    def observation_fact_process(self, mandatory_cols: dict, org_row: pd.Series, org_dwh_tbl_cat: pd.DataFrame) -> None:
        """
        Processes rows to observation_fact table, by applying specific rules from the data catalog. Each row is
        processed by taking values from the original row and applying them to the new row, which is then saved to the data
        warehouse table.

        Args:
            mandatory_cols (dict): A dictionary holding mandatory columns values for the row.
            org_row (pd.Series): The row from the original table.
            org_dwh_tbl_cat (pd.DataFrame): The relevant data catalog part for this original and data warehouse table.
        """
        mandatory_cols = self.add_encounter_num(mandatory_cols)
        # Add fields based on catalog rows with 'modifier_cd' == '@', don't exit if the value not exist.
        no_modifier_rows = org_dwh_tbl_cat[org_dwh_tbl_cat['modifier_cd'] == '@']
        if not no_modifier_rows.empty:
            for _, catalog_row in no_modifier_rows.iterrows():
                # TODO: In the future we need to merge the catalog rules that has the same 'concept_cd'
                self.process_row_by_observation_fact_rules(mandatory_cols, org_row, catalog_row)

        # Second loop: Process rows with 'modifier_cd' different from '@', if no 'modifier_cd' save row to dwh
        catalog_rows = org_dwh_tbl_cat[
            (org_dwh_tbl_cat['modifier_cd'].notna()) & (org_dwh_tbl_cat['modifier_cd'] != '@')]
        if not catalog_rows.empty:
            for _, catalog_row in catalog_rows.iterrows():
                self.process_row_by_observation_fact_rules(mandatory_cols, org_row, catalog_row)

    def process_row_by_observation_fact_rules(self, mandatory_cols: dict, org_row: pd.Series, catalog_row: pd.Series) -> None:
        """
        Saves the row to patient_dimension table, by the given data catalog rule.

        Args:
            mandatory_cols (dict): A dictionary holding mandatory columns values for the row.
            org_row (pd.Series): The row from the original table.
            catalog_row (pd.Series): The data catalog rule row.
        """
        new_row = mandatory_cols.copy()
        try:
            # Get column name in original table from which value will be taken
            org_col_name = catalog_row['column_name']
            # Skip the row if the original data column value is empty
            if pd.isna(org_row[org_col_name]):
                raise OriginalDataFieldIsMissing(org_col_name)
            # Assign value to relevant column in data warehouse table
            new_row[catalog_row['target_column']] = org_row[org_col_name]
            new_row['concept_cd'] = catalog_row['concept_cd']
            new_row['modifier_cd'] = catalog_row['modifier_cd']

            # 'tval_char' is t or n depending on the tval_char/nval_num
            if 'tval_char' in new_row:
                new_row['valtype_cd'] = 't'
            elif 'nval_num' in new_row:
                new_row['valtype_cd'] = 'n'

            # Check for words in hebrew and translate them
            new_row = self.translate_heb_words(new_row)
            self.validate_row(new_row)
            self.add_row_to_batch(new_row, self.current_dwh_table_name)
        except (OriginalDataFieldIsMissing, NotNullColumnIsMissing) as e:
            self.handle_exception(org_row, new_row, e)

    def get_mandatory_cols(self, org_dwh_tbl_cat: pd.DataFrame, org_row: pd.Series) -> dict:
        """
        Get the mandatory fields and values that required for that original data row.
        Args:
            org_dwh_tbl_cat (pd.DataFrame): The relevant data catalog part for this original and data warehouse table.
            org_row (pd.Series): The original data row being processed.

        Returns:
            dict: The mandatory columns that required for this data warehouse table.
        """
        # TODO: Those hardcoded fields need to be changed in production.
        mandatory_cols = {
            'update_date': datetime.now(),
            'download_date': datetime.now(),
            'import_date': datetime.now(),
            'sourcesystem_cd': 'reuma_v2',
            'upload_id': 1
        }
        # Identify mandatory columns like ["patient_num", "date", "provider_id"]
        mandatory_rows = org_dwh_tbl_cat[
            pd.isna(org_dwh_tbl_cat['concept_cd']) & pd.isna(org_dwh_tbl_cat['modifier_cd'])]

        # Iterate over each mandatory row in the catalog and populate the mandatory columns
        if not mandatory_rows.empty:
            for _, catalog_row in mandatory_rows.iterrows():
                org_col_name = catalog_row['column_name']
                if pd.notna(org_row[org_col_name]):
                    mandatory_cols[catalog_row['target_column']] = org_row[catalog_row['column_name']]
                else:
                    raise MandatoryFieldMissing(org_col_name)
        return mandatory_cols

    def translate_heb_words(self, row: dict) -> dict:
        """
        Searching for words in Hebrew in the row and translate them to English.
        If the word is missing in the data warehouse dictionary it translate the word using
        Translator and save the new word to the data warehouse dictionary

        Args:
            row (dict): The row with the potentially Hebrew words.

        Returns:
            dict: The same row after translation
        """
        dwh_engine = self.dwh_engine
        translated_row = {}
        # TODO: In the future, if we want to translate only specific field so we need to add a column check here.
        for key, value in row.items():
            # Check if the value is a string and contains Hebrew characters
            if isinstance(value, str) and re.search('[א-ת]', value):
                heb_word = value
                # Load data warehouse dictionary if not loaded
                if self.dwh_dictionary is None:
                    query = "SELECT * FROM dictionary"
                    self.dwh_dictionary = dwh_engine.fetch_data_from_db(query)

                dictionary = self.dwh_dictionary
                # Translate word from data warehouse dictionary
                if heb_word in dictionary['he'].values:
                    english_translation = dictionary.loc[dictionary['he'] == heb_word, 'en'].iloc[0]
                else:
                    # Translate word by Translator
                    english_translation = translate(heb_word)
                    # Save the new word to the dictionary table
                    new_word_translation = {'he': value, 'en': english_translation}
                    self.add_row_to_batch(new_word_translation, 'dictionary')
                    # Update the local dwh_dictionary
                    new_word_translation_df = pd.DataFrame([new_word_translation])
                    self.dwh_dictionary = pd.concat([dictionary, new_word_translation_df], ignore_index=True)

                translated_row[key] = english_translation
            else:
                # Keep non-string values as they are
                translated_row[key] = value

        return translated_row

    def add_encounter_num(self, mandatory_cols: dict) -> dict:
        """
        Add encounter number for the row heading to observation_fact based on 'start_date'.
        If the date doesn't exist in the encounters table, it creates a new encounter if it in the table.

        Args:
            mandatory_cols (dict): A dictionary holding mandatory columns values for the row.

        Returns:
            dict: A dictionary holding mandatory columns values for the row including 'encounter_num'.
        """
        # Load local encounter table if not loaded
        if self.encounters is None:
            query = "SELECT * FROM encounters"
            self.encounters = self.dwh_engine.fetch_data_from_db(query)
        encounters = self.encounters

        encounter_date = mandatory_cols['start_date'].date()
        # Set encounter_num if the date is already in encounter table
        if encounter_date in encounters['date'].values:
            encounter_num = encounters.loc[encounters['date'] == encounter_date, 'encounter_num'].iloc[0]
        else:
            # Set encounter_num and save the new date to encounter table
            encounter_num = encounters['encounter_num'].max() + 1
            new_encounter_date = {
                'date': encounter_date,
                'encounter_num': encounter_num
            }
            self.add_row_to_batch(new_encounter_date, 'encounters')

            # Update the local encounter table
            new_encounter_date_df = pd.DataFrame([new_encounter_date])
            self.encounters = pd.concat([encounters, new_encounter_date_df], ignore_index=True)

        # Update the encounter_num of the new row
        mandatory_cols['encounter_num'] = encounter_num
        return mandatory_cols

    def update_encounters_table(self, org_tables: dict) -> None:
        """
        Collects all the dates from all tables in the dictionary, converts them to the required format, removes
        duplicates, orders the values, creates the corresponding encounter_num values, saves the table to the dwh.

        Args:
            org_tables (dict): A dictionary with all the original tables
        """
        # Collect all dates from all original tables
        all_dates = []
        for table_name, table in org_tables.items():
            if 'Entry_Date' in table.columns:
                all_dates.extend(table['Entry_Date'])
        # Convert to 'Y-m-d' format, remove duplicates, and order values
        unique_dates = sorted(pd.to_datetime(all_dates).strftime('%Y-%m-%d').unique())
        encounters_nums = list(range(1, len(unique_dates) + 1))
        # Create a DataFrame with unique dates and corresponding encounter_num values
        encounters = pd.DataFrame({'date': unique_dates, 'encounter_num': encounters_nums})
        self.dwh_engine.save_new_row_to_dwh(encounters, 'encounters')
        print('encounters table updated')

    def validate_row(self, new_row: dict) -> None:
        """
        Checks if the NOT NULL columns in that row are populated according to the data warehouse table structure.
        If they are missing, it adds them to the 'validation_failed_run_log'.

        Args:
            new_row (dict): The new row to be added to the data warehouse table.
        """
        dwh_table_name = self.current_dwh_table_name
        for not_null_col in self.dwh_engine.get_not_null_columns(dwh_table_name):
            if not_null_col not in new_row or not new_row[not_null_col]:
                raise NotNullColumnIsMissing(not_null_col)

    def handle_exception(self, org_row: pd.Series, new_row: dict,
                         e: Union[MandatoryFieldMissing, NotNullColumnIsMissing, OriginalDataFieldIsMissing]) -> None:
        """
        Write exceptions to log files details about the missing value.
        Save the exception to 'exceptions' table.

        Args:
            org_row (pd.Series): The original data row being processed.
            new_row (dict): The new row we tried to create.
            e (MandatoryFieldMissing, NotNullColumnIsMissing, OriginalDataFieldIsMissing): Exception object.
        """
        dwh_table_name = self.current_dwh_table_name
        org_table_name = self.current_org_table_name
        log_file_id = None
        target_col = None
        org_col = None

        if isinstance(e, MandatoryFieldMissing) or isinstance(e, OriginalDataFieldIsMissing):
            # Called when a value is missing during the ETL process.
            org_row_string = ", ".join(f"{col}: {val}" for col, val in org_row.items())
            field_type = 'mandatory field' if isinstance(e, MandatoryFieldMissing) else 'field'
            org_col = e.org_col_name
            error_message = f"For '{dwh_table_name}', " \
                            f"the {field_type} '{org_col}' is missing in: " \
                            f"Table: '{org_table_name}' " \
                            f"Original row: ({org_row_string})\n"
            log_file_id = write_to_log_file(error_message)
        elif isinstance(e, NotNullColumnIsMissing):
            # Called when a new row fails the validation check for NOT NULL columns.
            target_col = e.not_null_col
            error_message = f"Validation failed for '{dwh_table_name}', '{target_col}' is missing: {new_row}\n"
            log_file_id = write_to_log_file(error_message)

        if log_file_id is not None:

            # Custom serialization function for datetime objects
            def serialize_datetime(obj):
                if isinstance(obj, datetime):
                    return obj.strftime('%Y-%m-%d %H:%M:%S')
                return None  # Return None for non-serializable objects

            self.add_row_to_batch({
                'log_file_id': log_file_id,
                'target_table': dwh_table_name,
                'org_table': org_table_name,
                'target_col': target_col,
                'org_col': org_col,
                'row_json': json.dumps(new_row, default=serialize_datetime)
            }, 'exceptions')

    def add_row_to_batch(self, new_row: dict, table_name: str) -> None:
        """
        Add the row to the tables que. This function makes sure the new data is saved by batches and not one by one.
        Args:
            new_row (dict): The new row we want to add to que.
            table_name(str): The name of the data warehouse table.
        """
        que = self.batch_que
        que_limit = 100

        # Update LogPrinter
        if table_name != 'exceptions':
            self.logger.dwh_row_process(table_name)
        else:
            self.logger.dwh_row_process(self.current_dwh_table_name, failed=True)

        # Ensure all dictionaries have the same set of keys
        table_cols = self.dwh_engine.get_columns(table_name)
        for col in table_cols:
            if col not in new_row:
                new_row[col] = None

        if table_name not in que:
            # Create a table que if the table not in que
            que[table_name] = [new_row]
        else:
            # Add row to tabl que
            que[table_name].append(new_row)

        if len(que[table_name]) >= que_limit:
            # Save rows to data warehouse and restart the que
            self.dwh_engine.save_new_row_to_dwh(que[table_name], table_name)
            que[table_name] = []

    def save_all_rows_in_que(self) -> None:
        """
        Run it when you are done processing all the original data so the rows that waits in the que will be added,
        """
        for table_name, table_que in self.batch_que.items():
            if len(table_que) > 0:
                self.dwh_engine.save_new_row_to_dwh(table_que, table_name)
