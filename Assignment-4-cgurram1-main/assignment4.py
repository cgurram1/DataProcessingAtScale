# Import required libraries
# Do not install/import any additional libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math 


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "assignment4"
host = "127.0.0.1"


def get_open_connection():
    """
    Connect to the database and return connection object
    
    Returns:
        connection: The database connection object.
    """

    return psycopg2.connect(f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'")



def load_data(table_name, csv_path, connection, header_file):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        connection: The database connection object.
        header_file (str): The path to where the header file is located
    """

    cursor = connection.cursor()

    # Creating the table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {table_rows_formatted}
            )'''

    cursor.execute(create_table_query)
    connection.commit()


    # Insert data using COPY
    with open(csv_path, 'r') as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
    connection.commit()

    print(f"Data loaded successfully into table {table_name}")




def range_partition(data_table_name, partition_table_name, num_partitions, header_file, column_to_partition, connection):
    """
    Partition the data in the given table using a range partitioning approach.
    
    Args:
        data_table_name (str): The name of the table that contains the data.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): Path to the header file containing column headers and data types.
        column_to_partition (str): The column to partition by.
        connection: The database connection object.
    """
    cursor = connection.cursor()

    # Step 1: Create the parent partition table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    # Creating the parent table without partitioning
    table_rows_formatted = (", ".join(f"{header} {header_type}" for header, header_type in header_dict.items()))
    create_parent_table_query = f'''
        CREATE TABLE IF NOT EXISTS {partition_table_name} (
            {table_rows_formatted}
        ) PARTITION BY RANGE ({column_to_partition});'''
    
    cursor.execute(create_parent_table_query)
    connection.commit()

    # Step 2: Get the min and max values for the column to partition
    cursor.execute(f'SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name};')
    min_value, max_value = cursor.fetchone()

    # Calculate range for partitions
    total_range = max_value - min_value + 1
    partition_range = math.ceil(total_range / num_partitions)

    # Step 3: Create partition tables
    for i in range(num_partitions):
        partition_min = min_value + i * partition_range
        partition_max = min_value + (i + 1) * partition_range - 1

        # Define the partition table
        partition_table_name_i = f"{partition_table_name}{i}"
        create_partition_query = f'''
            CREATE TABLE IF NOT EXISTS {partition_table_name_i} 
            PARTITION OF {partition_table_name} 
            FOR VALUES FROM ({partition_min}) TO ({partition_max + 1});'''
        
        cursor.execute(create_partition_query)

    connection.commit()

    # Step 4: Insert data into the partitioned tables
    insert_data_query = f'''
        INSERT INTO {partition_table_name}
        SELECT * FROM {data_table_name};'''

    cursor.execute(insert_data_query)
    connection.commit()

    cursor.close()
    print(f"Data partitioned into {num_partitions} range partitions.")



def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): Path to the header file containing column headers and data types.
        connection: The database connection object.
    """

    # Create the parent table for partitioning
    cursor = connection.cursor()
    with open(header_file) as schema_file:
        schema_dict = json.load(schema_file)

    # Define the columns and create the parent table
    columns_formatted = ", ".join(f"{col} {col_type}" for col, col_type in schema_dict.items())
    create_parent_table_query = f'''
        CREATE TABLE IF NOT EXISTS {partition_table_name} (
            {columns_formatted}
        );
    '''
    cursor.execute(create_parent_table_query)

    # Create child tables for round-robin partitioning
    for i in range(num_partitions):
        child_table_name = f"{partition_table_name}{i}"
        create_child_table_query = f'''
            CREATE TABLE IF NOT EXISTS {child_table_name} (
                LIKE {partition_table_name} INCLUDING ALL
            ) INHERITS ({partition_table_name});
        '''
        cursor.execute(create_child_table_query)

    # Create a sequence to keep track of the round-robin insertion
    sequence_name = f"{partition_table_name}_seq"
    cursor.execute(f"CREATE SEQUENCE IF NOT EXISTS {sequence_name} START 0 INCREMENT 1 MINVALUE 0")

    # Define the trigger function for round-robin insertion
    trigger_function_query = f'''
    CREATE OR REPLACE FUNCTION {partition_table_name}_insert_trigger()
    RETURNS TRIGGER AS $$
    DECLARE
        target_partition TEXT;
        partition_index INT;
    BEGIN
        -- Get the next partition index using the sequence
        partition_index := nextval('{sequence_name}') % {num_partitions};
        target_partition := '{partition_table_name}' || partition_index;

        -- Insert the row into the appropriate partition
        EXECUTE format('INSERT INTO %I VALUES ($1.*)', target_partition) USING NEW;
        RETURN NULL;  -- Prevents duplicate insertion into parent table
    END;
    $$ LANGUAGE plpgsql;
    '''
    cursor.execute(trigger_function_query)

    # Create the trigger on the parent table to call the trigger function on each insert
    trigger_query = f'''
    CREATE TRIGGER insert_round_robin_trigger
    BEFORE INSERT ON {partition_table_name}
    FOR EACH ROW EXECUTE FUNCTION {partition_table_name}_insert_trigger();
    '''
    cursor.execute(trigger_query)

    # Insert data from the original table into the parent table, invoking the round-robin trigger
    cursor.execute(f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}")

    # Commit the transaction and close the cursor
    connection.commit()
    cursor.close()
    print(f"Round-robin partitioning complete for {partition_table_name} with {num_partitions} partitions.")




def delete_partitions(table_name, num_partitions, connection):
    """
    This function in NOT graded and for your own testing convinience.
    Use this function to delete all the partitions that are created by you.

    Args:
        table_name (str): The name of the table containing the partitions to be deleted.
        num_partitions (int): The number of partitions to be deleted.
        connection: The database connection object.
    """

    # TODO: UNGRADED: Implement code to delete partitions here
    raise Exception("Function yet to be implemented!")

if __name__ == "__main__":
    connection = get_open_connection()
    load_data("subreddits", "subreddits.csv", connection, "headers.json")
    # range_partition("subreddits", "range_part", 5, "headers.json", "created_utc", connection)
    # Call the function with appropriate parameters
    # round_robin_partition("subreddits", "rrobin_part", 3, "headers.json", connection)
    # delete_partitions("range_partition", 5, connection)
    connection.close()