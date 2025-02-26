# create_connection()

This function establishes a connection to a PostgreSQL database and returns the connection object and cursor for executing queries. If the connection fails, it handles the error and returns None.


# close_connection(connection, cursor)

This function closes the provided database connection and cursor. If an error occurs during the closing process, it catches the exception and prints an error message.

# col_map()

This function defines two global variables: col_info and relation.
•	col_info: A dictionary that maps different sheet names (e.g., "Stores", "Users", "Routes") to a list of column mappings. Each column mapping specifies the DataFrame column name, the corresponding database column, the table name, and a label value.
•	relation: A dictionary that contains relationships between different tables (e.g., "Stores" related to "Regions" and "Branches"). Each relationship defines the columns and keys that connect tables in the database.
The function doesn't return anything but sets up these global variables for later use in the application.


# validate_excel(col_info, df_sheet, sheet_name)

This function validates an Excel sheet based on the column information defined in col_info. Here's a breakdown:
•	It first checks if the sheet name exists in col_info.
•	If the sheet name is found, it compares the actual columns in the sheet (df_sheet.columns) with the expected columns defined in col_info for that sheet.
•	If any expected columns are missing, it prints a message listing the missing columns and returns None.
•	If the validation is successful (i.e., all expected columns are present), it prints a success message and returns True.
The function is designed to ensure that the required columns are present in the sheet before proceeding further.

# compare_specific_column(sheet_column, db_column, table_name , label, df_sheet, sheet_name , cursor)

This function compares a specific column's values from the Excel sheet (df_sheet) with the corresponding values in a database table (table_name). Here's a breakdown of what it does:
1.	Condition based on the label: The function only proceeds if the label is equal to 1.
2.	Extract unique values from the sheet: It collects all unique values from the specified column (sheet_column) in the DataFrame (df_sheet), ignoring NaN values.
3.	Query database for distinct values: It executes an SQL query to get distinct values from the specified db_column in the given table_name from the database.
4.	Compare values: The function then compares the unique values from the sheet with the values fetched from the database. 
o	Missing values: If there are values present in the Excel sheet that are not found in the database, the function prints a message showing those values and returns False.
The purpose of this function is to ensure that all the values present in a specific column in the sheet exist in the corresponding database table. If any discrepancies are found, it returns False and alerts the user.

# check_values_in_db(excel_file, cursor, relation)

This function, check_values_in_db, checks whether the values from two specified columns in an Excel file match those from two columns in the database by joining two tables. Here's an overview of what the function does:
1.	Read Excel file: The function reads the specified Excel file (excel_file) into a Pandas DataFrame. It only reads the columns specified in relation (which contains the names of the columns to check).
2.	Construct SQL query: It dynamically constructs an SQL query that joins two tables (table1 and table2) on the specified keys (key_t1 and key_t2) and selects the specified columns (column1_db and column2_db) from those tables.
3.	Execute the query: The query is executed using the provided database cursor (cursor), and the results are fetched.
4.	Compare values: It then compares the values from the specified columns in the DataFrame (df) with those from the database:
o	For each row in the DataFrame, it checks if the tuple of values from the two columns exists in the results of the SQL query (join_set).
o	If any value pair does not match the database values, it prints a message with the row index and the mismatched values.
The function helps ensure that the data in the Excel file matches the database values by checking against two related tables. If any mismatched values are found, they are reported with the row index.

# compare_segments(cursor, route_code, user_code)

This function compare_segments checks whether the SegmentId from the Routes table matches the SegmentId related to the SalesmanType of a specific user. Here's a step-by-step explanation of how it works:
1.	Get SegmentId from Routes:
o	It first runs a query to fetch the SegmentId from the Routes table where the Code matches the provided route_code.
o	If no result is returned, the function immediately returns False.
2.	Get SegmentId from SalesmanTypes:
o	It then runs a second query to fetch the SegmentId from the SalesmanTypes table. This is based on the SalesmanTypeId associated with the user_code from the Users table.
o	If no result is returned, the function returns False.
3.	Comparison:
o	Finally, the function compares the two SegmentId values: one from the Routes table and one from the SalesmanTypes table. If they are equal, it returns True; otherwise, it returns False.
In summary, this function ensures that the segment of a user (determined by their SalesmanType) matches the segment of the route associated with the given route code.

# process_excel(file_path, col_info)

The process_excel function reads an Excel file, validates its sheets, and compares the data in those sheets against a database:
1.	Database Connection: It establishes a connection to the database using the create_connection function.
2.	Reading Excel File: The function reads the Excel file into a dictionary of DataFrames.
3.	Sheet Validation: It iterates over each sheet, checking for required columns using validate_excel. If the columns are invalid, it skips that sheet.
4.	Data Comparison: For each valid sheet, it compares the data between the Excel file and the database: 
o	For sheets like "Routes - Users", it checks if the segment for a user matches the segment for the route using the compare_segments function.
o	For the "Stores" sheet, it checks if specific values match in the database using the check_values_in_db function.
5.	Error Handling: If any errors occur during the process, they are caught and printed.
6.	Connection Cleanup: After processing, it closes the database connection and cursor.
The function helps ensure that data in the Excel file matches the data in the database across different sheets.

# __name__ == "__main__"

Here's a brief explanation of the code block:
1.	Database Configuration:
The db_config dictionary contains the configuration parameters required to connect to the database. This includes:
o	dbname: Name of the database.
o	user: Username for the database.
o	password: Password for the user.
o	host: The server address where the database is hosted.
o	port: The port number to connect to the database.
2.	File Path:
The file_path variable holds the path to the Excel file (Routesd.xlsx) that will be processed.
3.	Column Mapping:
The col_map() function is called to load the necessary column information into col_info.
4.	Process Excel File:
The process_excel() function is called, passing the file_path and the col_info for processing the data in the Excel file and comparing it with the database.
In short, this block runs the entire program by connecting to the database and processing the specified Excel file.

