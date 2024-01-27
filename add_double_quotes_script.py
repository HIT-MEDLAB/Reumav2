import tkinter as tk
from tkinter import filedialog


def quote_sql_identifiers(sql_content):
    """
        Process the provided SQL content to add double quotes around SQL identifiers.

        Args:
        - sql_content (str): The SQL content to be processed.

        Returns:
        - str: The processed SQL content with identifiers quoted.
    """
    sql_lines = sql_content.split('\n')
    # Define column types and create prefixes for them in both lowercase and uppercase
    column_types = ['bigint', 'int', 'smallint', 'timestamp', 'float', 'varchar', 'text', 'date', 'boolean', 'double',
                    'real']
    column_types = column_types + [item.upper() for item in column_types]
    column_types_suffixes = ['\t' + item for item in column_types] + [' ' + item for item in column_types]
    # Define pairs of prefixes and suffixes to identify where to add quotes
    pairs = [
        ('CREATE TABLE ', [' (']),
        ('PRIMARY KEY (', [')']),
        ('INSERT INTO ', [' VALUES']),
        ('\t, CONSTRAINT ', [' FOREIGN KEY']),
        ('CONSTRAINT ', [' FOREIGN KEY', ' PRIMARY KEY']),
        ('FOREIGN KEY (', [') REFERENCES']),
        ('REFERENCES ', ['(']),
        ('CREATE INDEX ', [' ON']),
        (' ON ', ['(']),
        # We better keep those as the last of the pairs
        ('\t', column_types_suffixes),
        ('"(', [')']),
        ('"(', [',']),
        ('",', [')']),
        ('",', [',']),
        ('  , ', ['\t']),
    ]
    # using 'while' and not 'for' the keep each line by reference to the original sql_lines
    i = 0
    while i < len(sql_lines):
        line = sql_lines[i]
        for prefix, suffixes in pairs:
            if prefix in line:
                for suffix in suffixes:
                    if suffix in line:
                        word_start = line.find(prefix) + len(prefix)
                        # Start searching the suffix only after the current prefix
                        word_end = line.find(suffix, word_start)
                        word = line[word_start:word_end]

                        # Special handling for INSERT INTO statements
                        if prefix == 'INSERT INTO ' and word[-1] == ')':
                            table_name, values = word.split(' ', 1)
                            word = table_name
                            word_end = word_end - len(values)
                        # Check if the word is not already quoted and doesn't contain certain characters
                        if '"' not in word and ',' not in word and '(' not in word:
                            word = word.strip()
                            line = line[:word_start] + '"' + word + '"' + line[word_end:]
                            sql_lines[i] = line  # Assign the modified line back to the list
        i = i + 1
        print(f'\033[94m{line}\033[0m', repr(line))
    print(sql_lines)
    sql_content = '\n'.join(sql_lines)
    return sql_content


def main():
    # Create a simple GUI for file selection
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    file_path = filedialog.askopenfilename(title="Select a SQL file", filetypes=[("SQL files", "*.sql")])
    if not file_path:
        print("No file selected. Exiting.")
        return

    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    # Quote the SQL identifiers
    quoted_content = quote_sql_identifiers(content)
    # Save to a new file
    new_file_path = file_path.replace('.sql', f'_quoted.sql')
    with open(new_file_path, 'w', encoding='utf-8') as file:
        file.write(quoted_content)
    print(f"Processed SQL saved to: {new_file_path}")


if __name__ == "__main__":
    main()
