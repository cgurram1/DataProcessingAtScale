# Import required libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math 


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "assignment5"
host = "127.0.0.1"



def point_query(parent_partition_table_name, utc_val, save_table_name, connection):
    """
    Use this function to perform a point query on the given table. 
    The table input is either range (range_part) or round-roublin (rrobin_part) partitioned.
    The output should be saved in a table with the name "save_table_name".
    Make sure the ouptu is stored in asc order

    Args:
        parent_partition_table_name (str): The name of the table containing the partitions to be queried.
        utc_val (str): The UTC value to be queried.
        save_table_name (str): The name of the table where the output is to be saved.
        connection: The database connection object.
    """

    try:
        cursor = connection.cursor()
        
        # Drop the save table if it already exists to avoid conflicts
        cursor.execute(f"DROP TABLE IF EXISTS {save_table_name}")
        
        # Create the save table
        cursor.execute(f"""
            CREATE TABLE {save_table_name} AS
            SELECT * 
            FROM {parent_partition_table_name}
            WHERE created_utc = %s
            ORDER BY created_utc ASC
        """, (utc_val,))
        
        # Commit the transaction
        connection.commit()
        print(f"Data successfully saved in table {save_table_name}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Rollback the transaction in case of an error
    finally:
        cursor.close()


def range_query(parent_partition_table_name, utc_min_val, utc_max_val, save_table_name, connection):
    """
    Use this function to perform a range query on the given table. 
    The table is either range (range_part) or round-roublin (rrobin_part) partitioned.
    The output should be saved in a table with the name "save_table_name".
    Make sure the ouptu is stored in asc order

    Args:
        parent_partition_table_name (str): The name of the table containing the partitions to be queried.
        utc_min_val (str): The minimum UTC value to be queried.
        utc_max_val (str): The maximum UTC value to be queried.
        save_table_name (str): The name of the table where the output is to be saved.
        connection: The database connection object.
    """

    try:
        cursor = connection.cursor()
        
        # Drop the save table if it already exists to avoid conflicts
        cursor.execute(f"DROP TABLE IF EXISTS {save_table_name}")
        
        # Create the save table
        cursor.execute(f"""
            CREATE TABLE {save_table_name} AS
            SELECT * 
            FROM {parent_partition_table_name}
            WHERE created_utc > %s AND created_utc <= %s
            ORDER BY created_utc ASC
        """, (utc_min_val, utc_max_val))
        
        # Commit the transaction
        connection.commit()
        print(f"Data successfully saved in table {save_table_name}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Rollback the transaction in case of an error
    finally:
        cursor.close()
    
