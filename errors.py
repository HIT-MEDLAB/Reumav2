class MandatoryFieldMissing(Exception):
    """
    Raises when mandatory field is missing in the original data.

    Args:
        org_col_name(str): The name of the missing mandatory field.
    """

    def __init__(self, org_col_name: str):
        self.org_col_name = org_col_name
        self.message = f"Mandatory field '{org_col_name}' is missing"
        super().__init__(self.message)


class NotNullColumnIsMissing(Exception):
    """
    Raises when not null field is missing in the row that about to be saved in the Database.

    Args:
        not_null_col(str): The name of the missing column.
    """

    def __init__(self, not_null_col: str):
        self.not_null_col = not_null_col
        self.message = f"NOT NULL field '{not_null_col}' is missing"
        super().__init__(self.message)


class OriginalDataFieldIsMissing(Exception):
    """
    Raises when original data field is missing in the row that about to be saved in the Database.

    Args:
        org_col_name(str): The name of the missing column.
    """

    def __init__(self, org_col_name: str):
        self.org_col_name = org_col_name
        self.message = f"Original data field '{org_col_name}' is missing"
        super().__init__(self.message)
