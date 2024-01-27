from datetime import datetime
from time import time


# TODO: Consider to limit the log file to certain size, because its a problem to work with log file bigger than 100mb...

def write_to_log_file(error_message: str) -> int:
    """
    Appends an error message to a log file, including an incrementing index.

    Args:
        error_message (str): The error message to be written to the log file.

    Returns:
        int: The index of the writen log row.
    """
    log_filename = f'{datetime.now().strftime("%Y-%m-%d")}-exceptions.log'
    # Count the number of rows in the log file and append the error message
    try:
        with open(f'logs/{log_filename}', 'a+', encoding='utf-8') as log_file:
            # Seek to the beginning of the file to count rows
            log_file.seek(0)
            row_index = sum(1 for _ in log_file) + 1
            # Append the error message with the row count
            log_file.write(f"{row_index}. {error_message}")
        return row_index
    except FileNotFoundError:
        # If the file doesn't exist, create it and write the error message
        with open(f'logs/{log_filename}', 'w', encoding='utf-8') as log_file:
            log_file.write(f"1. {error_message}")
        return 1


class LogPrinter:
    """
    Logs progress and metrics during ETL process. This class tracks overall progress of ETL process by logging start
    time, total rows, rows processed, end time and elapsed time.
    It also logs progress for each individual table insertion.

    Attributes:
        ttl_org_data_rows_count (int): Total rows in original tables to process.
        ttl_org_data_rows_processed (int): Running count of rows processed.
        tables_process_start_time (float): Start time of overall ETL process.
        specific_table_process_start_time (float): Start time for current table.
        org_table_currently_processing (str): The name of the current original data table been processed.
        ttl_table_rows_processed (int): The amount of rows that already processed from that original data table.
        ttl_table_rows_to_process (dict): A dictionary that holds the count of records from each original data table.
        dwh_rows_successfully_saved (dict): A dictionary that holds the count of all the records saved in the data
        warehouse for each data warehouse table, for a single original data table.
        dwh_rows_failed_to_save (dict): A dictionary that holds the count of all the count of the failed attempts to
        save a record to the data warehouse for each data warehouse table, for a single original data table.

    Methods:
        tables_process_started: Logs start of ETL process.
        specific_table_process_started: Logs start time of current table insertion.
        row_processed: Logs processing of each row.
        specific_table_process_ended: Logs end time and elapsed time for current table.
        tables_process_ended: Logs end time and elapsed time for overall ETL process.
    """
    ttl_org_data_rows_count = 0
    ttl_org_data_rows_processed = 0
    tables_process_start_time = None
    specific_table_process_start_time = None
    org_table_currently_processing = ''
    ttl_table_rows_processed = 0
    ttl_table_rows_to_process = {}
    dwh_rows_successfully_saved = {}
    dwh_rows_failed_to_save = {}

    def __init__(self, ttl_org_data_rows_count: int):
        """
        Initialize the LogPrinter.

        Args:
            ttl_org_data_rows_count (int): Total rows in original tables to process.
        """
        self.ttl_org_data_rows_count = ttl_org_data_rows_count
        self.ttl_org_data_rows_processed = 0

    def tables_process_started(self) -> None:
        """
        Log start time of overall ETL process.
        """
        self.tables_process_start_time = time()
        print(f"Total rows to process: {self.ttl_org_data_rows_count}")

    def specific_table_process_started(self, org_table_name: str, ttl_table_rows_to_process: int) -> None:
        """
        Log start time of current table insertion.

        Args:
            org_table_name (str): The name of the current original table being processed.
            ttl_table_rows_to_process (int): Total rows count of current original table being processed.
        """
        self.specific_table_process_start_time = time()
        self.org_table_currently_processing = org_table_name
        self.ttl_table_rows_to_process[org_table_name] = ttl_table_rows_to_process
        self.ttl_table_rows_processed = 0
        self.dwh_rows_successfully_saved[org_table_name] = {}
        self.dwh_rows_failed_to_save[org_table_name] = {}
        print(f"\033[94mWorking on {org_table_name}\033[0m- {ttl_table_rows_to_process} row to process.")

    def dwh_row_process(self, dwh_table_name: str, failed=False) -> None:
        """
        Update the dictionaries that record the data warehouse row process

        Args:
            dwh_table_name (str): The name of the data warehouse table the row belongs to.
            failed (bool): If the row failed to save in the data warehouse.
        """
        # Get the right dictionary according to the status
        if failed:
            dic_to_save = self.dwh_rows_failed_to_save[self.org_table_currently_processing]
        else:
            dic_to_save = self.dwh_rows_successfully_saved[self.org_table_currently_processing]

        if dwh_table_name in dic_to_save:
            dic_to_save[dwh_table_name] = dic_to_save[dwh_table_name] + 1
        else:
            dic_to_save[dwh_table_name] = 1

    def row_processed(self) -> None:
        """
        Log processing of each row.
        """
        self.ttl_org_data_rows_processed = ttl_org_data_rows_processed = self.ttl_org_data_rows_processed + 1
        self.ttl_table_rows_processed = ttl_table_rows_processed = self.ttl_table_rows_processed + 1
        ttl_table_rows_to_process = self.ttl_table_rows_to_process[self.org_table_currently_processing]
        # Calculate general elapsed time
        general_elapsed_time = time() - self.tables_process_start_time
        minutes, seconds = divmod(general_elapsed_time, 60)  # Convert seconds to minutes and seconds
        # Calculate process percentage
        total_etl_process_percent = round(((ttl_org_data_rows_processed / self.ttl_org_data_rows_count) * 100), 2)
        table_etl_process_percent = round(((ttl_table_rows_processed / ttl_table_rows_to_process) * 100), 2)
        # Update progress for every row processed
        print(f"Table process on: \t\033[94m{table_etl_process_percent}%\033[0m,\t"
              f"Total ETL process on: \033[93m{total_etl_process_percent}%\033[0m, "
              f"already running for {int(minutes)}:{int(seconds):02d} minutes.", end='\r',
              flush=True)

    def specific_table_process_ended(self, org_table_name: str) -> None:
        """
        Log end time and elapsed time for current table insertion.

        Args:
            org_table_name (str): The name of the current original table being processed.
        """
        ttl_table_rows = self.ttl_table_rows_to_process[org_table_name]
        # Time sum-up
        elapsed_time = time() - self.specific_table_process_start_time  # Compute the elapsed time
        minutes, seconds = divmod(elapsed_time, 60)  # Convert seconds to minutes and seconds

        # A dictionary hold the status of dwh records count created for the current org table
        dwh_rows_dict = {
            'success': self.dwh_rows_successfully_saved,
            'failed': self.dwh_rows_failed_to_save
        }
        table_sumup_msg = ''

        # Fill the org table sum-up message according to records status
        for status, rows_dict in dwh_rows_dict.items():
            # The records for the current org table
            if len(rows_dict[org_table_name]) > 0:
                dwh_tables_rows = rows_dict[org_table_name]
                # Total records created in the ETL process for all the tables in the data warehouse
                ttl_rows = sum(dwh_tables_rows.values())
                # Creates a string is the following structure: '<dwh table name>': <records created count>
                tbls_sumup_str = "\n".join(f"|\t'{tbl_name}': \t{count}" for tbl_name, count in dwh_tables_rows.items())

                status_str = 'new rows created in' if status == 'success' else 'rows failed to enter'
                table_sumup_msg += f"\n| {ttl_rows} {status_str} the data warehouse:\n{tbls_sumup_str}"

        # Print table sum-up
        print(f"Done with {org_table_name}. Time taken: {int(minutes)}:{int(seconds):02d} minutes.\n"
              f"| Out of {ttl_table_rows} medical records {table_sumup_msg}")
        # Print % of ETL process
        print(f"\033[93m{round(((self.ttl_org_data_rows_processed / self.ttl_org_data_rows_count) * 100), 2)}%\033[0m\n")

        self.specific_table_process_start_time = None

    def tables_process_ended(self) -> None:
        """
        Log end time and elapsed time for overall ETL process.
        """
        # Calculate general elapsed time
        general_elapsed_time = time() - self.tables_process_start_time
        minutes, seconds = divmod(general_elapsed_time, 60)
        self.tables_process_start_time = None
        print(f"Process original table into DWH complete- runtime: {int(minutes)}:{int(seconds):02d} minutes.")
