# Assignment-1-cgurram1

# README

## Overview

This script automates the process of creating tables, loading data from `.csv` files, establishing relationships, and running queries on a PostgreSQL database. It is intended for use in assignments involving database creation, data loading, and querying.

## Prerequisites

- **PostgreSQL** must be installed on your system.
- Ensure you have access to a PostgreSQL user (`postgres` in this case) with appropriate privileges.
- Make sure that the `.sql` files (`create_tables.sql`, `create_relations.sql`, `queries.sql`) and the `.csv` data files (`authors.csv`, `subreddits.csv`, `submissions.csv`, `comments.csv`) are present in the same directory as the script or provide the correct path.

## Usage

1. **Create Tables**: The script first runs the `create_tables.sql` file to create necessary database tables.

2. **Load Data into Tables**: It uses the `\COPY` command to load data from `.csv` files into the corresponding tables:
   - `authors.csv` → `authors` table
   - `subreddits.csv` → `subreddits` table
   - `submissions.csv` → `submissions` table
   - `comments.csv` → `comments` table

3. **Create Relationships**: After loading the data, it runs the `create_relations.sql` file to establish relationships between the tables.

4. **Run Queries**: Finally, it executes the `queries.sql` file to run predefined SQL queries on the database.

## Running the Script

Execute the script in a terminal with the appropriate privileges:

In Terminal:
    chmod +x assignment1.sh
    ./assignment1.sh


## Notes

- Ensure that all `.csv` files are correctly formatted and match the table schema.
- The script assumes the database name is `postgres`. Modify the script if you are using a different database name.

## Output

The script will print messages indicating the progress of each step, including the creation of tables, data loading, relationship creation, and query execution.