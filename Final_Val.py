import psycopg2
import pandas as pd
import logging
import os

# Set up Logger for error logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Use environment variables to avoid storing the password in the code
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "2-16MD"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "Pa$$w0rdahmed1996"),  # ⚠️ Store it in an environment variable instead of the code
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# Establish a connection to the database
def create_connection():
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        logging.info("✅ Connected to the database successfully!")
        return connection, cursor
    except Exception as e:
        logging.error(f"❌ Error connecting to the database: {e}")
        return None, None  # Avoid returning an uncontrolled `NoneType`

# Close the database connection
def close_connection(connection, cursor):
    if connection and cursor:
        cursor.close()
        connection.close()
        logging.info("🔌 Connection closed.")

# Load column mappings and relationships
def col_map():
    col_info = {
            "Stores": [
                {"DataFrameColumn": "Store Code", "DatabaseColumn": "Code", "Table": "Stores" , "checkwDB":1},
                {"DataFrameColumn": "Store Name", "DatabaseColumn": "EnName", "Table": "Stores" , "checkwDB":0},
                {"DataFrameColumn": "Region", "DatabaseColumn": "EnName", "Table": "Regions" , "checkwDB":1},
                {"DataFrameColumn": "Branch", "DatabaseColumn": "EnName", "Table": "Branches" , "checkwDB":1},
                {"DataFrameColumn": "Site", "DatabaseColumn": "EnName", "Table": "Sites" , "checkwDB":1},
                {"DataFrameColumn": "Channel", "DatabaseColumn": "EnName", "Table": "Channels" , "checkwDB":1},
                {"DataFrameColumn": "Sub Channel", "DatabaseColumn": "EnName", "Table": "SubChannels" , "checkwDB":1},
                {"DataFrameColumn": "SR Code", "DatabaseColumn": "Code", "Table": "Users" , "checkwDB":0},
                {"DataFrameColumn": "Chain", "DatabaseColumn": "EnName", "Table": "Chains" , "checkwDB":1},
                {"DataFrameColumn": "Area Code", "DatabaseColumn": "Code", "Table": "Areas" , "checkwDB":0},
                {"DataFrameColumn": "RE Segmantation", "DatabaseColumn": "Name", "Table": "RESegmantations" , "checkwDB":1},
                {"DataFrameColumn": "Sub RE Segmantation", "DatabaseColumn": "Name", "Table": "SubReSegmentations" , "checkwDB":1},
                {"DataFrameColumn": "Longitude", "DatabaseColumn": "Longitude", "Table": "Stores" , "checkwDB":0},
                {"DataFrameColumn": "Latitude", "DatabaseColumn": "Latitude", "Table": "Stores" , "checkwDB":0},
                {"DataFrameColumn": "Tiers", "DatabaseColumn": "Tier", "Table": "Stores" , "checkwDB":1},
                {"DataFrameColumn": "DC/SD", "DatabaseColumn": "SD_DC", "Table": "Stores" , "checkwDB":1},
                {"DataFrameColumn": "Sales office ID", "DatabaseColumn": "Code", "Table": "SalesOffices" , "checkwDB":1},
                {"DataFrameColumn": "Address", "DatabaseColumn": "Address", "Table": "Stores" ,  "checkwDB":0},
                {"DataFrameColumn": "Frequency", "DatabaseColumn": "Frequency", "Table": "Stores" ,  "checkwDB":0},
                {"DataFrameColumn": "Day", "DatabaseColumn": "Day", "Table": "Stores" ,  "checkwDB":0},
                {"DataFrameColumn": "Day Dry", "DatabaseColumn": "DayDry", "Table": "Stores" ,  "checkwDB":0},
                
            ],
            "Users": [
                {"DataFrameColumn": "UserName", "DatabaseColumn": "UserName", "Table": "Users" , "checkwDB":1},
                {"DataFrameColumn": "First Name", "DatabaseColumn": "FirstName", "Table": "Users" , "checkwDB":0},
                {"DataFrameColumn": "Last Name", "DatabaseColumn": "LastName", "Table": "Users" , "checkwDB":0},
                {"DataFrameColumn": "Default Role", "DatabaseColumn": "Description", "Table": "SalesmanTypes" , "checkwDB":1},
                {"DataFrameColumn": "Reset Password", "DatabaseColumn": "Password", "Table": "Users" , "checkwDB":0},
                {"DataFrameColumn": "Sales Man Type", "DatabaseColumn": "Name", "Table": "SalesmanTypes" , "checkwDB":1},
        
            ],
            "Routes": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "checkwDB":1},
                {"DataFrameColumn": "Route Name", "DatabaseColumn": "Name", "Table": "Routes" , "checkwDB":1},
                {"DataFrameColumn": "Route Type", "DatabaseColumn": "Type", "Table": "Routes" , "checkwDB":1},
            ],
            "Routes - Stores": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "checkwDB":1},
                {"DataFrameColumn": "Store Number", "DatabaseColumn": "Code", "Table": "Stores" , "checkwDB":1}
            ],
            "Routes - Users": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "checkwDB":1},
                {"DataFrameColumn": "Username", "DatabaseColumn": "UserName", "Table": "Users" , "checkwDB":1}
            ]}

    # Define relationship mapping here without using global variables
    relation = {"Stores": [
                    {
                      "column1_db": "EnName",
                      "column2_db": "EnName",
                      "column1": "Region",
                      "column2": "Branch",
                      "table1": "Regions",
                      "table2": "Branches",
                      "key_t1": "Id",
                      "key_t2": "RegionId"
                    },
                    {
                      "column1_db": "EnName",
                      "column2_db": "EnName",
                      "column1": "Branch",
                      "column2": "Site",
                      "table1": "Branches",
                      "table2": "Sites",
                      "key_t1": "Id",
                      "key_t2": "BranchId"
                    },
                    {
                      "column1_db": "EnName",
                      "column2_db": "Code",
                      "column1": "Site",
                      "column2": "Area Code",
                      "table1": "Sites",
                      "table2": "Areas",
                      "key_t1": "Id",
                      "key_t2": "SiteId"
                    },   
                      
                    {
                      "column1_db": "Name",
                      "column2_db": "Name",
                      "column1": "RE Segmantation",
                      "column2": "Sub RE Segmantation",
                      "table1": "RESegmantations",
                      "table2": "SubReSegmentations",
                      "key_t1": "Id",
                      "key_t2": "RESegmantationId"
                    },   
                      
                    {
                      "column1_db": "Name",
                      "column2_db": "Code",
                      "column1": "Sub RE Segmantation",
                      "column2": "Store Code",
                      "table1": "SubReSegmentations",
                      "table2": "Stores",
                      "key_t1": "Id",
                      "key_t2": "SubReSegmentationId"
                    }
                  ]}  

    return col_info, relation
# Checking for missing columns
def validate_excel(col_info, df_sheet, sheet_name):
    logging.info(f"🔍 Validating sheet: {sheet_name}")
    
    if sheet_name not in col_info:
        logging.warning(f"⚠️ Sheet '{sheet_name}' not in expected templates.")
        return False

    expected_cols = [col["DataFrameColumn"] for col in col_info[sheet_name]]
    actual_cols = df_sheet.columns.tolist()

    missing_columns = [col for col in expected_cols if col not in actual_cols]

    if missing_columns:
        logging.error(f"❌ Missing columns in '{sheet_name}': {missing_columns}")
        return False

    logging.info(f"✅ Sheet '{sheet_name}' validated successfully.")
    return True

def compare_specific_column(sheet_column, db_column, table_name, checkwDB, df_sheet, sheet_name, cursor):
    if sheet_name == 'Routes - Users':
        df_sheet["Username"] = df_sheet["Username"].str.split("@", expand=True)[0]

    if checkwDB == 1:
        
        sheet_unique_values = set(df_sheet[sheet_column].dropna().apply(str).str.strip().str.lower().unique())

        query = f'SELECT DISTINCT LOWER("{db_column}") FROM public."{table_name}"'
        cursor.execute(query)
        db_values = {row[0].strip() for row in cursor.fetchall() if row[0] is not None}  # Handling None values

        values_in_sheet_not_in_db = sheet_unique_values - db_values

        if values_in_sheet_not_in_db:
            print(f"❌ Values {list(values_in_sheet_not_in_db)} exist in column '{sheet_column}' in file '{sheet_name}' but are not found in the database.")
            return False

    return True  # No errors found

def compare_segments(cursor, route_code, code, user_or_store):

    try:
        query1 = """
            SELECT "SegmentId"
            FROM public."Routes"
            WHERE "Code" = %s
            LIMIT 1;
        """
        cursor.execute(query1, (route_code,))
        segment_id_route = cursor.fetchone()

        if segment_id_route is None:
            return False  # No value found

        if user_or_store == 0:
            query2 = """
                SELECT "SegmentId"
                FROM public."SalesmanTypes"
                WHERE "Id" IN (
                    SELECT "SalesmanTypeId"
                    FROM public."Users"
                    WHERE "Code" = %s
                )
                LIMIT 1;
            """
        else:
            query2 = """
                SELECT "SegmentId"
                FROM public."Channels"
                WHERE "Id" IN (
                    SELECT "ChannelId"
                    FROM public."SubChannels"
                    WHERE "Id" IN (
                        SELECT "SubChannelId"
                        FROM public."Chains"
                        WHERE "Id" IN (
                            SELECT "ChainId"
                            FROM public."Stores"
                            WHERE "Code" = %s
                        )
                    )
                )
                LIMIT 1;
            """

        cursor.execute(query2, (code,))
        segment_id_comparison = cursor.fetchone()

        if segment_id_comparison is None:
            return False

        return segment_id_route[0] == segment_id_comparison[0]

    except Exception as e:
        print(f"⚠ Error while comparing segments: {e}")
        return False


def get_segments(cursor):
    """ Fetch all SegmentIds for Routes, Users, and Stores in one go and store them in dictionaries. """
    segments = {
        "routes": {},
        "users": {},
        "stores": {}
    }

    # Get all Route SegmentIds
    cursor.execute('SELECT "Code", "SegmentId" FROM public."Routes"')
    segments["routes"] = {str(row[0]): row[1] for row in cursor.fetchall()}

    # Get all User SegmentIds via SalesmanTypes
    cursor.execute('''
        SELECT U."Code", S."SegmentId" 
        FROM public."Users" U
        JOIN public."SalesmanTypes" S ON U."SalesmanTypeId" = S."Id"
    ''')
    segments["users"] = {str(row[0]): row[1] for row in cursor.fetchall()}

    # Get all Store SegmentIds via Channels
    cursor.execute('''
        SELECT ST."Code", C."SegmentId"
        FROM public."Stores" ST
        JOIN public."Chains" CH ON ST."ChainId" = CH."Id"
        JOIN public."SubChannels" SC ON CH."SubChannelId" = SC."Id"
        JOIN public."Channels" C ON SC."ChannelId" = C."Id"
    ''')
    segments["stores"] = {str(row[0]): row[1] for row in cursor.fetchall()}

    return segments

def compare_segments_optimized(route_code, code, user_or_store, segments):
    """ Compare SegmentIds using pre-fetched data instead of running a query each time. """
    segment_id_route = segments["routes"].get(route_code)
    segment_id_comparison = segments["users"].get(code) if user_or_store == 0 else segments["stores"].get(code)

    if segment_id_route is None or segment_id_comparison is None:
        return False

    return segment_id_route == segment_id_comparison






def check_values_in_db(excel_file, cursor, relation, batch_size=10000):
    # Load the Excel file into a DataFrame
    df = pd.read_excel(excel_file, usecols=[relation["column1"], relation["column2"]])

    excel_dict = {(str(row[relation["column1"]]), str(row[relation["column2"]])) for _, row in df.iterrows()}

    query = f"""
    SELECT R."{relation["column1_db"]}", B."{relation["column2_db"]}"
    FROM public."{relation["table1"]}" R
    JOIN public."{relation["table2"]}" B ON R."{relation["key_t1"]}" = B."{relation["key_t2"]}"
    """

    cursor.execute(query)

    db_dict = set()
    
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break  

        db_dict.update((str(row[0]), str(row[1])) for row in batch)

    differences = excel_dict - db_dict

    for index, (value1, value2) in enumerate(differences):
        print(f"Row {index}: In column {relation['column1']}, {relation['column2']} - The values {value1}, {value2} do not relate!")

    return


# def check_values_in_db(excel_file, cursor, relation):
#     # Read the Excel file into a DataFrame with the provided columns
#     df = pd.read_excel(excel_file, usecols=[relation["column1"], relation["column2"]])

#     # Dynamically construct the query to join tables based on the provided columns
#     query = f"""
#     SELECT R."{relation["column1_db"]}" as "col1", B."{relation["column2_db"]}" as "col2" 
#     FROM public."{relation["table1"]}" R
#     JOIN public."{relation["table2"]}" B ON R."{relation["key_t1"]}" = B."{relation["key_t2"]}"
#     """
#     # Execute the query
#     cursor.execute(query)
#     # print(query)
#     # Fetch the results from the JOIN
#     join_results = cursor.fetchall()

#     join_set = set(join_results)

#     for index, row in df.iterrows():
#         value1 = str(row[relation["column1"]])
#         value2 = str(row[relation["column2"]])
#         # print(index)
        
#         if (str(value1), str(value2)) not in join_set:
#             # differences.append((index ,relation["column1"] , relation["column2"] ,   value1, value2 ))
#             print(f"Row {index}: In column {relation['column1']}, {relation['column2']} - The values {value1}, {value2} do not relate!.")
  
#     return

def save_differences_to_excel(differences, filename="differences.xlsx"):
    """ Save the list of dictionaries to an Excel file """
    if differences:
        df = pd.DataFrame(differences)
        df.to_excel(filename, index=False)
        print(f"✅ Differences saved to {filename}")
    else:
        print("✅ No differences found, no file saved.")


# Process the entire Excel file
def process_excel(file_path, col_info):
    connection, cursor = create_connection()
    if cursor is None:
        logging.error("❌ Unable to connect to the database. Exiting...")
        return

    try:
        data = pd.read_excel(file_path, sheet_name=None, dtype=str)

        for sheet_name, df_sheet in data.items():
            if not validate_excel(col_info, df_sheet, sheet_name):
                continue  # Skip the sheet if it's not valid

            tables_columns = col_info.get(sheet_name, [])
            for row in tables_columns:
                compare_specific_column(row["DataFrameColumn"], row["DatabaseColumn"], row["Table"], row["checkwDB"], df_sheet, sheet_name, cursor)
            
            # all_differences = []

            if sheet_name == "Routes - Users":
                if 'Route Id' in df_sheet.columns and 'Username' in df_sheet.columns:
                    df_sheet["Username"] = df_sheet["Username"].str.split("@", expand=True)[0]
                    differ_seg = []

                    segments = get_segments(cursor)  

                    for index, row in df_sheet.iterrows():
                        route_code = str(row['Route Id'])
                        user_code = str(row['Username'])

                        is_equal = compare_segments_optimized(route_code, user_code, user_or_store=0, segments=segments)
                        if not is_equal:
                            differ_seg.append({"Index": index, "Route Code": route_code, "User Code": user_code})
 
                    save_differences_to_excel(differ_seg, f"segment_differences_{sheet_name}.xlsx")            
            if sheet_name == "Routes - Stores":
                if 'Route Id' in df_sheet.columns and 'Store Number' in df_sheet.columns:
                    differ_seg = []

                    segments = get_segments(cursor)  

                    for index, row in df_sheet.iterrows():
                        route_code = str(row['Route Id'])
                        store_code = str(row['Store Number'])

                        is_equal = compare_segments_optimized(route_code, store_code, user_or_store=1, segments=segments)
                        if not is_equal:
                            differ_seg.append({ "Index": index, "Route Code": route_code, "Store Code": store_code})

                    save_differences_to_excel(differ_seg, f"segment_differences_{sheet_name}.xlsx")
                        
            # print("test")
            # Finally, compare the data between the Excel sheet and the database
            if sheet_name == "Stores":
                for relation_item in relation["Stores"]:
                    check_values_in_db(excel_file=file_path, cursor=cursor, relation=relation_item)

    except Exception as e:
        logging.error(f"❌ Error processing Excel file: {e}")

    finally:
        close_connection(connection, cursor)


# Execute the program
if __name__ == "__main__":
    file_path = "Routes Feb'2025.xlsx"
    col_info, relation = col_map()
    process_excel(file_path, col_info)
