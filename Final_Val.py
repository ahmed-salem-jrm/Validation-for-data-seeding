import psycopg2
import pandas as pd
def create_connection():
    try:
        connection = psycopg2.connect(
            host="localhost",       # Server address
            dbname="2-16MD",        # Database name
            user="postgres",        # Username
            password="Pa$$w0rdahmed1996"  # Password
        )
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to close connection and cursor
def close_connection(connection, cursor):
    try:
        cursor.close()
        connection.close()
        print("close connection!")
    except Exception as e :
        print("Error:",e)


##################################################
def col_map():
    #we can use return 
    global col_info
    col_info = {
            "Stores": [
                {"DataFrameColumn": "Store Code", "DatabaseColumn": "Code", "Table": "Stores" , "label":1},
                {"DataFrameColumn": "Store Name", "DatabaseColumn": "EnName", "Table": "Stores" , "label":0},
                {"DataFrameColumn": "Region", "DatabaseColumn": "EnName", "Table": "Regions" , "label":1},
                {"DataFrameColumn": "Branch", "DatabaseColumn": "EnName", "Table": "Branches" , "label":1},
                {"DataFrameColumn": "Site", "DatabaseColumn": "EnName", "Table": "Sites" , "label":1},
                {"DataFrameColumn": "Channel", "DatabaseColumn": "EnName", "Table": "Channels" , "label":1},
                {"DataFrameColumn": "Sub Channel", "DatabaseColumn": "EnName", "Table": "SubChannels" , "label":1},
                {"DataFrameColumn": "SR Code", "DatabaseColumn": "Code", "Table": "Users" , "label":0},
                {"DataFrameColumn": "Chain", "DatabaseColumn": "EnName", "Table": "Chains" , "label":1},
                {"DataFrameColumn": "Area Code", "DatabaseColumn": "Code", "Table": "Areas" , "label":0},
                {"DataFrameColumn": "RE Segmantation", "DatabaseColumn": "Name", "Table": "RESegmantations" , "label":1},
                {"DataFrameColumn": "Sub RE Segmantation", "DatabaseColumn": "Name", "Table": "SubReSegmentations" , "label":1},
                {"DataFrameColumn": "Longitude", "DatabaseColumn": "Longitude", "Table": "Stores" , "label":0},
                {"DataFrameColumn": "Latitude", "DatabaseColumn": "Latitude", "Table": "Stores" , "label":0},
                {"DataFrameColumn": "Tiers", "DatabaseColumn": "Tier", "Table": "Stores" , "label":1},
                {"DataFrameColumn": "DC/SD", "DatabaseColumn": "SD_DC", "Table": "Stores" , "label":1},
                {"DataFrameColumn": "Sales office ID", "DatabaseColumn": "Code", "Table": "SalesOffices" , "label":1},
                {"DataFrameColumn": "Address", "DatabaseColumn": "Address", "Table": "Stores" ,  "label":0},
                {"DataFrameColumn": "Frequency", "DatabaseColumn": "Frequency", "Table": "Stores" ,  "label":0},
                {"DataFrameColumn": "Day", "DatabaseColumn": "Day", "Table": "Stores" ,  "label":0},
                {"DataFrameColumn": "Day Dry", "DatabaseColumn": "DayDry", "Table": "Stores" ,  "label":0},
                
            ],
            "Users": [
                {"DataFrameColumn": "UserName", "DatabaseColumn": "UserName", "Table": "Users" , "label":1},
                {"DataFrameColumn": "First Name", "DatabaseColumn": "FirstName", "Table": "Users" , "label":1},
                {"DataFrameColumn": "Last Name", "DatabaseColumn": "LastName", "Table": "Users" , "label":1},
                {"DataFrameColumn": "Default Role", "DatabaseColumn": "Description", "Table": "SalesmanTypes" , "label":1},
                {"DataFrameColumn": "Reset Password", "DatabaseColumn": "Password", "Table": "Users" , "label":1},
                {"DataFrameColumn": "Sales Man Type", "DatabaseColumn": "Name", "Table": "SalesmanTypes" , "label":1},
        
            ],
            "Routes": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "label":1},
                {"DataFrameColumn": "Route Name", "DatabaseColumn": "Name", "Table": "Routes" , "label":1},
                {"DataFrameColumn": "Route Type", "DatabaseColumn": "Type", "Table": "Routes" , "label":1},
            ],
            "Routes - Stores": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "label":1},
                {"DataFrameColumn": "Store Number", "DatabaseColumn": "Code", "Table": "Stores" , "label":1}
            ],
            "Routes - Users": [
                {"DataFrameColumn": "Route Id", "DatabaseColumn": "Code", "Table": "Routes" , "label":1},
                {"DataFrameColumn": "Username", "DatabaseColumn": "UserName", "Table": "Users" , "label":1}
            ]
        }
    global relation
    relation = {
                  "Stores": [
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
                  ]
                }
    
def validate_excel(col_info, df_sheet, sheet_name):
    print(f"Validating sheet: {sheet_name}")
    
    if sheet_name not in col_info:
        print(f"Sheet '{sheet_name}' does not exist in the templates.")
        # sys.exit("Error: Sheet name does not match the expected format.")  
        return 

    expected_cols = col_info[sheet_name]
    # print(expected_cols["DataFrameColumn"])
    # print("########################3")
    actual_cols = df_sheet.columns.tolist()

    missing_columns = [col["DataFrameColumn"] for col in expected_cols if col["DataFrameColumn"] not in actual_cols]

    if missing_columns:
        print(f"❌ Missing columns in sheet '{sheet_name}': {missing_columns}")
        # sys.exit("Error: The sheet has missing required columns. Please fix and try again.")  
        return
    print(f"✅ Sheet '{sheet_name}' validated successfully.")
    return True


################################################################################################

# compare_specific_column(row["DataFrameColumn"], row["DatabaseColumn"], row["Table"], df_sheet, sheet_name ,  cursor)
                    
def compare_specific_column(sheet_column, db_column, table_name , label, df_sheet, sheet_name , cursor):
    if label == 1:
        sheet_unique_values = set(df_sheet[sheet_column].apply(str).dropna().unique())
        
        query = f'SELECT DISTINCT "{db_column}" FROM public."{table_name}"'
        cursor.execute(query)
        db_values = set([str(row[0]) for row in cursor.fetchall()])
    
        values_in_sheet_not_in_db = sheet_unique_values - db_values
    
        if values_in_sheet_not_in_db:
            print(f"❌ Values {list(values_in_sheet_not_in_db)} found in column '{sheet_column}' in Sheet '{sheet_name}' but not in database.")
            # sys.exit(f"Error: Some values in '{sheet_column}' do not exist in the database. Please fix and try again.")
            return False
        # print(f"✅ All values are present in the database.")
###############################################################################################

def check_values_in_db(excel_file, cursor, relation):
    # Read the Excel file into a DataFrame with the provided columns
    df = pd.read_excel(excel_file, usecols=[relation["column1"], relation["column2"]])

    # Dynamically construct the query to join tables based on the provided columns
    query = f"""
    SELECT R."{relation["column1_db"]}" as "col1", B."{relation["column2_db"]}" as "col2" 
    FROM public."{relation["table1"]}" R
    JOIN public."{relation["table2"]}" B ON R."{relation["key_t1"]}" = B."{relation["key_t2"]}"
    """
    # Execute the query
    cursor.execute(query)
    # print(query)
    # Fetch the results from the JOIN
    join_results = cursor.fetchall()

    join_set = set(join_results)

    for index, row in df.iterrows():
        value1 = str(row[relation["column1"]])
        value2 = str(row[relation["column2"]])
        # print(index)
        
        if (str(value1), str(value2)) not in join_set:
            # differences.append((index ,relation["column1"] , relation["column2"] ,   value1, value2 ))
            print(f"Row {index}: in column {relation["column1"] , relation["column2"]} The values {value1 , value2} do not match any in the database.")
  
    return 

################################################################################################
def compare_segments(cursor, route_code, user_code):
    # First query: Get SegmentId from Routes based on the Code
    query1 = """
        SELECT "SegmentId"
        FROM public."Routes"
        WHERE "Code" = %s;
    """
    cursor.execute(query1, (route_code,))
    segment_id_route = cursor.fetchone()

    if segment_id_route is None:
        return False  # If no value is found, return False

    # Second query: Get SegmentId from SalesmanTypes based on the SalesmanTypeId from Users
    query2 = """
        SELECT "SegmentId"
        FROM public."SalesmanTypes"
        WHERE "Id" IN (
            SELECT "SalesmanTypeId"
            FROM public."Users"
            WHERE "Code" = %s
        );
    """
    cursor.execute(query2, (user_code,))
    segment_id_salesman_type = cursor.fetchone()

    if segment_id_salesman_type is None:
        return False  # If no value is found, return False

    # Compare the two values
    return segment_id_route[0] == segment_id_salesman_type[0]

###############################################################################################
def process_excel(file_path, col_info):
    try:
        connection , cursor = create_connection()
        # print(cursor)
        if cursor is None:
            print("Unable to connect to the database.....")
            return
        # Read the Excel file
        data = pd.read_excel(file_path, sheet_name=None)
        
        for sheet_name in data.keys():
            df_sheet = data.get(sheet_name)

            # Validate required columns in the sheet
            if not validate_excel(col_info, df_sheet, sheet_name ):
                print(f"Invalid columns in sheet {sheet_name}.")
                continue  # Skip this sheet if validation fails

            # print(sheet_name)
            if sheet_name:
                # Generate the necessary table columns for comparison
                tables_columns = col_info[sheet_name]
                # Check if tables_columns is not empty before proceeding
                if tables_columns:                    
                    for row  in tables_columns:
                        compare_specific_column(row["DataFrameColumn"], row["DatabaseColumn"], row["Table"] , row["label"], df_sheet, sheet_name ,  cursor)
                    if sheet_name== "Routes - Users":
                        if 'Route Id' in df_sheet.columns and 'Username' in df_sheet.columns:
                            # df_sheet["Username"] = df_sheet["Username"].str.split("@" , expand = True)[0]
                            differ_seg = []
                            for index, row in df_sheet.iterrows():
                                route_code = str(row['Route Id'])
                                user_code = str(row['Username'])
                                # Call compare_segments function with the extracted route_code and user_code
                                is_equal = compare_segments(cursor, route_code, user_code)
                                if is_equal == False:
                                    differ_seg.append({"index":index ,  "route_code":route_code, "user_code":user_code  })
                            if differ_seg :
                                print("there are different segments:" ,differ_seg)
                            else:
                                print("Same segment")
                    
                    
                    # Finally, compare the data between the Excel sheet and the database
                    if sheet_name == "Stores":
                        for  i in relation["Stores"]:
                            check_values_in_db(excel_file = file_path, cursor = cursor , relation = i)    
                else:
                    print(f"No columns found to compare for sheet {sheet_name}. Skipping.")
            else:
                print(f"Skipping sheet {sheet_name}.")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the connection and cursor
        close_connection(connection, cursor)

# Main execution block
if __name__ == "__main__":
    # Configuration for database connection
    db_config = {
        'dbname': '2-16MD',
        'user': 'postgres',
        'password': 'Pa$$w0rdahmed1996',
        'host': 'localhost',
        'port': '5432'
    }

    # Run the program on a specific Excel file
    file_path = "Routesd.xlsx"
    col_map()
    process_excel(file_path, col_info)