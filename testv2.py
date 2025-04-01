import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import re

# Load environment variables from .env file
load_dotenv()

def generate_input_string_from_excel(file_path, sheet_name,entity):
    try:
        # Read the Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        
        # Ensure required columns exist
        required_columns = {"columnid", "entity","extrafieldname", "extrafieldtype", "defaultvalue"}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Missing required columns in the Excel sheet. Expected columns: {required_columns}")
        
        # Fill NaN values with empty strings to avoid issues
        df = df.fillna('')
        
        # Filter rows where entity matches the provided value
        df = df[df["entity"] == entity]

        # Generate the input string
        input_strings = [f"{row['columnid']}-{row['extrafieldtype']}-{row['extrafieldname']}-`{row['defaultvalue']}`"
                         for _, row in df.iterrows()]
        
        # Join all entries with '~'
        input_string = "~".join(input_strings)
        
        return input_string
    
    except Exception as e:
        print(f"Error: {e}")
        return None

def case_insensitive_mapping(default_values, distinct_values):
    """
    Checks if any distinct value can be mapped to the default values case-insensitively.
    Returns a list of mappings.
    """
    mappings = []
    for distinct_value in distinct_values:
        for default_value in default_values:
            if distinct_value.lower() == default_value.lower() and distinct_value != default_value:
                mappings.append((distinct_value, default_value))
    return mappings

def process_column_and_generate_query(input_string, output_file, table_name, account_id):
    try:
        # Get the database credentials from environment variables

        server = os.getenv('PGSQL_SERVER')
        database = os.getenv('PGSQL_DATABASE')
        username = os.getenv('PGSQL_USER')
        password = os.getenv('PGSQL_PASSWORD')
        schema=  os.getenv('PGSQL_schema')
        port = os.getenv('PGSQL_PORT')
        
        # Define the connection string
        conn_str = f"host={server} dbname={database} user={username} password={password} port={port}"

        # Connect to SQL Server
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()

        # Parse input string
        entries = input_string.split("~")
        for entry in entries:
            # Use regex to parse the input correctly
            pattern = r"^(.*?)\-(.*?)\-(.*?)\-`(.*?)`$"
            match = re.match(pattern, entry)
            if not match:
                print(f"Error: Invalid entry format for: {entry}")
                continue

            column_id, column_type, fieldName, default_values_string = match.groups()
            column_name_full = 'custcolumn' + column_id

            # Initialize entity_type_id with a default value based on the table name
            entity_type_id = {
                "candidate_custom_data": 5,
                "contact_custom_data": 2,
                "company_custom_data": 3,
                "job_custom_data": 4,
                "deal_custom_data": 13
            }.get(table_name)

            if not entity_type_id:
                print(f"Error: Table name '{table_name}' is not recognized, cannot proceed.")
                return
            
            # Handle dropdown and multiselect types
            if column_type in ['dropdown', 'multiselect']:

                # Query to get distinct values from the column
                query = f"SELECT DISTINCT {column_name_full} FROM {schema}.{table_name} ;"

                cursor.execute(query)
                results = cursor.fetchall()

                # Fetch distinct values
                distinct_values = [row[0] for row in results if row[0] is not None]

                if column_type == "dropdown" and any("," in value for value in distinct_values):
                    print(f"Alert: {column_name_full} contains a comma while type is dropdown!")
                    return

                if column_type == "multiselect":
                    dbcol_values = [v for val in distinct_values for v in val.split(",") if v.strip()]
                else:
                    dbcol_values = distinct_values

                alerts = []  # Store alerts instead of printing immediately

                for value in dbcol_values:
                    if re.search(r"^\s|\s$|\n|\t", value):
                        alerts.append(f"Alert: Value '{value}' has leading/trailing spaces, tabs, or newlines!")
                    if re.search(r"[\\]", value):
                        alerts.append(f"Alert: Value '{value}' contains escape characters!")

                if alerts:  # Print all alerts at once
                    print("\n".join(alerts))
                    return

                # Normalize case, strip whitespace, and deduplicate
                normalized_values = [v.strip() for v in dbcol_values if v.strip()]
                unique_values = sorted(set(normalized_values))

                default_value = ",".join(unique_values)
                escaped_default_value = default_value.replace("'", "''")  # Use double single quotes for SQL safety

                insert_query = (
                    f"INSERT INTO tblextrafields (accountid,columnid, extrafieldtype, extrafieldname, entitytypeid, defaultvalue) "
                    f"VALUES ({account_id},{column_id}, '{column_type}', '{column_name_full}', {entity_type_id}, '{escaped_default_value}');"
                )

                with open(output_file, "a", encoding="utf-8") as file:
                    file.write(insert_query + "\n")
            
            else:
                # Handle other column types
                insert_query = (
                    f"INSERT INTO tblextrafield (accountid,columnid, extrafieldtype, extrafieldname, entitytypeid, defaultvalue) "
                    f"VALUES ({account_id},{column_id}, '{column_type}', '{fieldName}', {entity_type_id}, NULL);"
                )
                with open(output_file, "a", encoding="utf-8") as file:
                    file.write(insert_query + "\n")

    except psycopg2.DatabaseError as err:
        print(f"Database Error: {err}")
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(f"Database Error: {err}\n")

    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(f"An unexpected error occurred: {ex}\n")

    finally:
    # Safely close the cursor if it exists
        if 'cursor' in locals() and cursor:
            try:
                cursor.close()
            except Exception as ex:
                print(f"Error while closing cursor: {ex}")

    # Safely close the connection if it exists
        if 'connection' in locals() and connection:
            try:
                connection.close()
            except Exception as ex:
                print(f"Error while closing connection: {ex}")

def process_column_and_update_query(input_string, Update_file, log_file, mappable_file, table_name, account_id):
    try:
        # Get database credentials from environment variables
        server = os.getenv('PGSQL_SERVER')
        database = os.getenv('PGSQL_DATABASE')
        username = os.getenv('PGSQL_USER')
        password = os.getenv('PGSQL_PASSWORD')
        schema=  os.getenv('PGSQL_schema')
        port = os.getenv('PGSQL_PORT')
        
        # Define the connection string
        conn_str = f"host={server} dbname={database} user={username} password={password} port={port}"

        # Connect to SQL Server
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()

        # Parse input string
        entries = input_string.split("~")
        for entry in entries:
            # Improved regex to correctly parse input
            pattern = r"^(.*?)\-(.*?)\-(.*?)\-(.*)$"
            match = re.match(pattern, entry)
            if not match:
                print(f"Error: Invalid entry format for: {entry}")
                continue

            column_id, column_type, fieldName, default_values_string = match.groups()
            column_name_full = f'custcolumn{column_id}'

            # Get entity type ID based on table name
            entity_type_id = {
                "candidate_custom_data": 5,
                "contact_custom_data": 2,
                "company_custom_data": 3,
                "job_custom_data": 4,
                "deal_custom_data": 13
            }.get(table_name)

            if not entity_type_id:
                print(f"Error: Unrecognized table name '{table_name}'.")
                return

            # Check if column exists
            query = f"""SELECT 1
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE column_name = '{column_name_full}' 
                        AND table_name = '{table_name}' 
                        AND table_schema = '{schema}'
                        and table_catalog ='{database}'; 
                    """
            cursor.execute(query)
            result = cursor.fetchone()
            if not result:
                continue            
            
            # Handle dropdown & multiselect
            if column_type in ['dropdown', 'multiselect']:
                default_values = [value.strip().strip('`') for value in default_values_string.split(",") if value.strip()]

                # Get distinct column values
                query = f'SELECT DISTINCT {column_name_full} FROM {schema}.{table_name}'
                cursor.execute(query)
                results = cursor.fetchall()
                distinct_values = [row[0] for row in results if row[0] is not None]

                if column_type=="dropdown"  and any("," in value for value in distinct_values):  
                    print(f"Alert: {column_name_full} contains a comma while type is dropdown!")
                    return

                if column_type == "multiselect":
                    dbcol_values = [v for val in distinct_values for v in val.split(",") if v.strip()]
                else:
                    dbcol_values = distinct_values

                alerts = []  # Store alerts instead of printing immediately

                for value in dbcol_values:
                    if re.search(r"^\s|\s$|\n|\t", value):
                        alerts.append(f"Alert: Value '{value}' has leading/trailing spaces, tabs, or newlines!")
                    if re.search(r"[\\]", value):
                        alerts.append(f"Alert: Value '{value}' contains escape characters!")

                if alerts:  # Print all alerts at once
                    print("\n".join(alerts))
                    return
                    
                dbcol_values = sorted(set(value.strip() for value in dbcol_values if value.strip()))
                
                
                # Check for case-insensitive mappings
                mappable_values = case_insensitive_mapping(default_values, dbcol_values)
                if mappable_values:
                        for uservalue, dbvalue in mappable_values:
                            with open(mappable_file, "a", encoding="utf-8") as output_3_file:
                                output_3_file.write(f"The db Value in {column_name_full} '{dbvalue}' can be mapped to '{uservalue}'\n")
                        continue
                        
                existing_values = [value for value in default_values if value in dbcol_values]
                new_values = [value for value in dbcol_values if value not in default_values]

                # Log existing default values
                if existing_values:
                    with open(log_file, "a", encoding="utf-8") as output2_file:
                        output2_file.write(f"Existing default values in {column_name_full}: {', '.join(existing_values)}\n")
                
                if new_values:
                    with open(log_file, "a", encoding="utf-8") as output2_file:
                        output2_file.write(f"New default values in {column_name_full}: {', '.join(new_values)}\n")
                

                # Combine & sort default values
                default_value = ",".join(sorted(set(filter(None, default_values + new_values))))
                escaped_default_value = default_value.replace("'", "\\'")

                # Generate update query
                update_query = (
                        f"UPDATE tblextrafields SET defaultvalue='{escaped_default_value}' "
                        f"WHERE accountid={account_id} AND columnid={column_id} AND entitytypeid={entity_type_id};"
                    )

                with open(Update_file, "a", encoding="utf-8") as file:
                    file.write(update_query + "\n")

    except psycopg2.DatabaseError as err:
        print(f"Database Error: {err}")
        with open(log_file, "w", encoding="utf-8") as file:
            file.write(f"Database Error: {err}\n")

    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
        with open(log_file, "w", encoding="utf-8") as file:
            file.write(f"An unexpected error occurred: {ex}\n")

    finally:
    # Safely close the cursor if it exists
        if 'cursor' in locals() and cursor:
            try:
                cursor.close()
            except Exception as ex:
                print(f"Error while closing cursor: {ex}")

    # Safely close the connection if it exists
        if 'connection' in locals() and connection:
            try:
                connection.close()
            except Exception as ex:
                print(f"Error while closing connection: {ex}")

# Main logic: Fetch input_string and table_name from environment variables
input_string = os.getenv('INPUT_STRING')
table_name = os.getenv('TABLE_NAME')
accountd_id= os.getenv('ACCOUNT_ID')
Excel_path= os.getenv('EXCEL_PATH')
database_name=os.getenv('DB_DATABASE')
entity='Candidate' # on which entity you are working

if Excel_path!='':
    excel_generated_string= generate_input_string_from_excel(Excel_path, 'extra_field_mapping',entity)
else: excel_generated_string=''


if excel_generated_string!='':
    process_column_and_update_query(
        input_string=excel_generated_string,
        Update_file="UpdateQuery.txt",
        log_file="Log.txt",
        mappable_file="Mappable_values.txt",
        table_name=table_name,
        account_id=accountd_id
    ) 

if not input_string or not table_name:
    print("Error: INPUT_STRING and TABLE_NAME must be set in the .env file.")
else:
    process_column_and_generate_query(
        input_string=input_string,
        output_file="ExtrafieldQuery.txt",
        table_name=table_name,
        account_id=accountd_id
    )
