# Import Libraries
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, NumericType
import pandas as pd

from databricks.sdk.runtime import *

class QualityControl:
    '''Initialize quality control functionality to perform on data tables.'''

    database_name = None

    def __init__(self, database_name='hive_metastore', schema_name='quality_control', refresh=False):
        """
        Initialize the class with specified database (Datalake Unite Catalog) name and schema

        @params
        database_name = name of the unity catalog to use for qc data
        schema_name = name of the schema to use for qc data
        refresh = boolean to refresh the qc table data in the specified schema
        """
        self.database = None
        self.schema_name = None
        
        # Set database name for any QC outputs
        self.set_database_name(database_name)

        # Clear out all data tables in specified schema, this removes test input data as well and should be used with caution
        if refresh:
            self.refresh_db(schema_name)        

    # Utility functions
    def set_database_name(self, database_name=None):
        '''
        Sets the Unity Catalog database to use when reading and writing process delta tables.
        '''    
        # Set the catalog
        if database_name is None:
            pass  # defaults to using hive_metastore which is always provided when using databricks workspaces
        else:
            # Set the desired catalog to use (i.e. 'potential_dev', 'potential_prod', etc.)
            sql(f"USE CATALOG {database_name}")

        # Retrieve and display current catalog, set as class attribute
        current_catalog = sql("SELECT current_catalog()").collect()[0][0]
        self.database = current_catalog

    def refresh_db(self, schema_name: str):
        """
        Refresh the qc tables in the specified schema.
        """
        # Get the list of tables in the specified schema
        sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.schema_name = schema_name

        tables = spark.sql(f"SHOW TABLES IN {self.schema_name}").collect()
        
        # For any tables in the quality control schema, empty contents
        for table in tables:
            table_name = table["tableName"]
            
            # Check if the table is a Delta table
            table_full_name = f"{schema_name}.{table_name}"
            table_properties = spark.sql(f"DESCRIBE DETAIL {table_full_name}").collect()[0].asDict()
            
            if table_properties.get('format') == 'delta':
                # If it is a Delta table, remove all rows
                sql(f"DELETE FROM {table_full_name}")

    def read_csv_from_subfolder(self, filename, folder_path, headers=None):
        # Get the directory of the current script
        
        # Construct the file path to the CSV file in the 'test_data' subfolder
        csv_file_path = os.path.join(folder_path, f'{filename}.csv')
        
        # Read the CSV file into a Pandas DataFrame with headers or not
        df = pd.read_csv(csv_file_path, header=headers)
        
        return df

    def correct_data_types(self, sdf1, sdf2):
        """
        Function to correct data types of a dataframe when loading in nested headers
        """
        if len(sdf1.columns) == len(sdf2.columns):
            header_tuple = list(zip(sdf1.columns, sdf2.columns))

            # Create a list of final column names
            final_names = [sdf_data_col if sdf_col.startswith("_&_") else sdf_col for sdf_col, sdf_data_col in header_tuple]

            # Update pdf_data columns with final_names
            col_index = 0
            for column in sdf2.columns:
                col_name = final_names[col_index]
                sdf2 = sdf2.withColumnRenamed(column, col_name)
                col_index += 1

            return sdf2
        else:
            return None     

    def get_qc_csv(self, filename, folder_path, count_nested_headers, header_indexes_to_ignore):
        """
        Load in a csv using steps required for qc testing. 
        
        Specify the number of nested headers and the indexes of any nested headers you do not want to include. Use two csv files reads on the same file to identify the headers and the data portions of raw data.

        @params
        filename: the name of the .csv file to load
        folder_path: the path to the subfolder of input data, relative to current script
        @returns
        flattened_sdf: pyspark dataframe of the csv data with flattened headers
        original_row_count: the number of rows in the original .csv file, including any header rows
        original_col_count: the number of columns in the original .csv file, including any header columns
        """

        # Get test data and Create pyspark dataframe with a single flattened header
        pdf = self.read_csv_from_subfolder(filename, folder_path, headers=None)
        raw_count = pdf.shape[0]

        # Check if the inputs are within the edge cases for the flatten_nested_headers function
        if (count_nested_headers == 0) or (count_nested_headers >= raw_count - 1) or (len(header_indexes_to_ignore) > count_nested_headers):
            # Set to default headers if incorrect input specified, set output to None tuple
            pdf_data = self.read_csv_from_subfolder(filename, folder_path, headers=None)
            flattened_sdf, original_row_count, original_col_count = [None, None, None]
        else:
            # read data
            pdf_data = self.read_csv_from_subfolder(filename, folder_path, headers=count_nested_headers-1)

            # Convert to pyspark dataframe for downstream use
            sdf_data = spark.createDataFrame(pdf_data) 
            
            # Run the flatten_nested_headers function and correct data types
            flattened_sdf, original_row_count, original_col_count = self.flatten_nested_headers(pdf, count_nested_headers, header_indexes_to_ignore)
            flattened_sdf = self.correct_data_types(flattened_sdf, sdf_data) 

        return flattened_sdf, original_row_count, original_col_count               

    # Core Functionality
    def flatten_nested_headers(self, df, count_nested_headers, header_indexes_to_ignore):
        """
        Function to flatten nested headers of table into standard non-nested column headers. 
        
        # Indicate the number of rows of nested headers and if any of the header rows should be ignored.   

        @params
        df: pandas dataframe of the table to flatten
        count_nested_headers: the number of rows or nested headers present in the .csv file
        header_indexes_to_ignore: list of nested header indexes that you can ignore or do not want included in the flattened column header

        @returns
        sdf: pyspark dataframe of the table with flattened headers

        contributions by: Ally Dugan, allyson.dugan@cadmusgroup.com  
        """
        header_indexes = [x for x in list(range(count_nested_headers)) if x not in header_indexes_to_ignore] 

        # Read in the csv with NO headers. will clean the nested headers as just rows in the dataframe
        original_row_count = df.shape[0]
        original_col_count = df.shape[1]

        # Delete the header rows you want to ignore
        df.drop(header_indexes_to_ignore, axis=0, inplace=True)

        # Collapse the multiple nested header rows into a single concatenated row with a '_' separator
        df_headers = df.iloc[header_indexes]  # Dataframe that just has the rows with the nested header rows
        df_headers.fillna('', inplace=True)  # Replace nulls in dataframe with blank strings
        df_concat_headers = df_headers.apply('_&_'.join, axis=0)

        # Apply the concatenated nested headers as the actual column headers for the dataframe
        df.columns = df_concat_headers
          
        # Drop the remaining header rows and reset index
        df.drop(header_indexes, axis=0, inplace=True)
        df.reset_index(drop=True, inplace=True)
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()

        # Convert to pyspark dataframe for downstream use
        sdf = spark.createDataFrame(df)

        return sdf, original_row_count, original_col_count 

    def generate_summary_table(self, df, table_name: str):
        '''
        Create a table that summarizes all built in QC checks.

        Generate qc fields for all columns of input table. Input qc fields include:
        1. If a string type field, Count of Distinct values
        2. IF a numeric field, Sum of entire column
        3. If a numeric field, Count of negative values
        4. If a numeric field, Count of positive values
        5. Total row count
        6. Total null count for each field

        TODO: Expand functionality to checks on additional datatype fields such as DateTime, boolean, etc.
        
        @params
        df: pyspark dataframe of table to summarize QC checks on
        table_name: str name of the summary table

        @returns
        summary_df: pyspark dataframe containing QC check summary values of each column in the input dataframe
        '''
        # Iterate through all of the columns in the table and build QC checks
        summary_list = []
        for field in df.schema.fields:
            # For string types, count distinct values
            if isinstance(field.dataType, StringType):
                summary = df.select(
                    lit(field.name).alias("Column"),
                    countDistinct(col(field.name)).alias("UniqueValues"),
                    lit(None).cast("double").alias("Sum"),  # Defaults to None for string type
                    lit(None).cast("long").alias("NegativeCount"),  # Defaults to None for string type
                    lit(None).cast("long").alias("PositiveCount"),  # Defaults to None for string type
                    count("*").alias("TotalCount"),
                    count(when(isnull(col(field.name)), True)).alias("NullCount")
                )
            # For numeric types, generate a summation of entire column
            elif isinstance(field.dataType, NumericType):
                summary = df.select(
                    lit(field.name).alias("Column"),
                    lit(None).cast("long").alias("UniqueValues"),
                    sum(col(field.name)).alias("Sum"),
                    count(when(col(field.name) < 0, True)).alias("NegativeCount"),
                    count(when(col(field.name) > 0, True)).alias("PositiveCount"),
                    count("*").alias("TotalCount"),
                    count(when(isnull(col(field.name)), True)).alias("NullCount")
                )
            else:
                # Skip columns that are neither StringType nor NumericType
                continue
            summary_list.append(summary)
        
        # Combine all summaries into one DataFrame
        summary_df = summary_list[0]
        for col_summary in summary_list[1:]:
            summary_df = summary_df.unionByName(col_summary)
        
        # Add the table name column
        summary_df = summary_df.withColumn("TableName", lit(table_name))

        return summary_df    
    
    def compare_tables(self, table1, table1_name: str, table2, table2_name: str):
        '''
        Accumulate logical comparison checks between two input tables.

        Comparison checks include the following steps:
        1. Check if Schemas of the two tables match
        2. Check if the row counts of the two tables match
        3. Check any common columns between the two tables for common content
        4. Generate a summary table for each table to add to output

        @params
        table1: pyspark dataframe containing first table to compare
        table1_name: str to set name of first table being compared
        table2: pyspark dataframe containing second table to compare
        table2_name: str to set name of second table being compared
        @returns
        results: dictionary of logical checks
        '''
        # TODO: Fix why ininstance does not work for verifying pypark dataframe datatype
        # if isinstance(table1, DataFrame) and isinstance(table2, DataFrame):
        #     pass
        # else:
        #     raise TypeError("Both table inputs must be a PySpark DataFrame")

        if type(table1) == type(table2):
            pass
        else:
            raise TypeError("Both table inputs must be a PySpark DataFrame")        
    
        # Step 1: Check if schemas match
        schema1 = set((field.name, field.dataType) for field in table1.schema.fields)
        schema2 = set((field.name, field.dataType) for field in table2.schema.fields)
        schemas_match = schema1 == schema2
        
        # Step 2: Compare the number of rows
        row_count1 = table1.count()
        row_count2 = table2.count()
        rows_match = row_count1 == row_count2

        # Step 3: Compare the values for common columns and verify content matches
        common_columns = [col for col, dtype in schema1 & schema2]
        if common_columns:
            table1_common = table1.select(*common_columns).dropDuplicates()
            table2_common = table2.select(*common_columns).dropDuplicates()
            # Check if data matches
            data_match = table1_common.exceptAll(table2_common).isEmpty() and table2_common.exceptAll(table1_common).isEmpty()
        else:
            print("No common columns to compare data.")
            data_match = False
        
        # Step 4: Output Summary table for each Delta table
        summary_table1 = self.generate_summary_table(table1, table1_name)
        summary_table2 = self.generate_summary_table(table2, table2_name)
        
        # Return results
        results = {
            "schemas_match": schemas_match,
            "rows_match": rows_match,
            "data_match": data_match,
            "all_match": schemas_match and rows_match and data_match,
            "summary_table1": summary_table1,
            "summary_table2": summary_table2,  # Expect future additions
        }
        return results


# This Section is for testing purposes only and can show examples of core functionality
if __name__ == "__main__":
    # Initialize the class
    quality_control = QualityControl()
