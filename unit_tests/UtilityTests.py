# Import Libraries
import importlib
import os
import unittest
import pandas as pd
from pyspark.sql.functions import sum

# Reload the module - required in Databricks env if any changes to class
module_name = 'src.utilities.QualityControl'
your_module = importlib.import_module(module_name)
importlib.reload(your_module)

# Import the class
from src.utilities.QualityControl import QualityControl

class UtilityTest(unittest.TestCase):
    """
    Run tests on core functionality of the Utility classes and methods based on expected output and behavior.

    Class Testing Include:
    1. QualityControl class
    2. TBD
    """
    def setUp(self):
        """
        Configure resources required for testing of the Utility Classes and Methods
        """
        # Initialize Test input variables and set up QualityControl Class
        self.database = "hive_metastore"  # default: 'hive_metastore'
        self.schema_name = "utility_test_data"  # default: 'quality_control'
        self.quality_control = QualityControl(self.database, self.schema_name)
        self.test_directory = "test_data"  # Path/Name of the local directory containing the test data
        self.flatten_headers_filename = "test_input_flatten_headers"  # Name of raw csv table to load test data from for Flatten Headers testing
        self.summary_fields_table_name = "test_input_summary_table"  # Name of delta table to load test data from for Summary Field testing
        self.compare_tables_control_table_name = "test_output_compare_tables"  # Name of delta table to load test data from for table comparison testing
        self.compare_tables_test_allMatch_table_name = "test_output_compare_tables"  # Name of delta table to load test data forcing NO match fail
        self.compare_tables_test_schema_table_name = "test_input_compare_tables_schema_match_fail"  # Name of delta table to load test data forcing schema match fail
        self.compare_tables_test_row_table_name = "test_input_compare_tables_row_match_fail"  # Name of delta table to load test data forcing row match fail
        self.compare_tables_test_data_table_name = "test_input_compare_tables_data_match_fail"  # Name of delta table to load test data forcing data match fail

        # Build Test Suite for the QualityControl.compare_tables function
        # Control Case: Match ALL Expected Test Results
        self.compare_tables_test_all_match_results = {
            "schemas_match": True,
            "rows_match": True,
            "data_match": True,
            "all_match": True
        }
        # Schema Match Expected Test Results
        self.compare_tables_test_schema_match_results = {
            "schemas_match": False,
            "rows_match": True,
            "data_match": True,
            "all_match": False
        }
        # Rows Match Expected Test Results
        self.compare_tables_test_row_match_results = {
            "schemas_match": True,
            "rows_match": False,
            "data_match": False,
            "all_match": False
        }
        # Data Match Expected Test Results
        self.compare_tables_test_data_match_results = {
            "schemas_match": True,
            "rows_match": True,
            "data_match": False,
            "all_match": False
        }
        # Set test suite with the expected results per test case
        self.compare_tables_test_test_suite = {
            self.compare_tables_test_allMatch_table_name: self.compare_tables_test_all_match_results,
            self.compare_tables_test_schema_table_name: self.compare_tables_test_schema_match_results,
            self.compare_tables_test_row_table_name: self.compare_tables_test_row_match_results,
            self.compare_tables_test_data_table_name: self.compare_tables_test_data_match_results,
        }

    def flatten_headers_check_size(self, test_count_nested_headers, test_header_indexes_to_ignore):
        """
        Sub function used to load in data and fix header AND assoicated data types
        """
        # Get test input variables from setUp
        test_filename = self.flatten_headers_filename
        test_folder_path = self.test_directory

        # Load in test case 'flattened_sdf' and get row and col count of df after flattening headers
        flattened_sdf, original_row_count, original_col_count = self.quality_control.get_qc_csv(
            test_filename, test_folder_path, test_count_nested_headers, test_header_indexes_to_ignore)

        if [flattened_sdf, original_row_count, original_col_count] == [None, None, None]:
            return None, None, None, None
        else:
            row_count = flattened_sdf.count()
            col_count = len(flattened_sdf.columns)

            # Calculate Expected Results After Flattening Headers
            qc_row_count = original_row_count - test_count_nested_headers - len(test_header_indexes_to_ignore)

            return qc_row_count, original_col_count, row_count, col_count

    def test_qc_flatten_headers(self):
        """
        Check that the dataframe is the expected size after the headers have been flattened

        Row and Column Count are verified.
        qc_row_count = original_row_count - count_nested_headers - lenth of header_indexes_to_ignore
        where,
        original_row_count (14) = count of all rows in original file, including any headers
        count_nested_headers (4) = number of rows in the table that are nested headers
        header_indexes_to_ignore = list of header rows to remove prior to flattening

        Edge Cases:
        The integer input: count_nested_headers == 0, resulting in no flattened headers and incorrect datatype interpretation
        The integer input: count_nested_headers is equal to the rows in the table, resulting in flattening of all data into headers and row count == 0
        The integer input: count_nested_headers is greater than the rows in the table, resulting in error
        The length of the header_indexes_to_ignore array is greater than count_nested_headers
        The length of the header_indexes_to_ignore array is greater than the rows in the table
        """
        # Load in test case 'flattened_sdf' and get row and col count of df after flattening headers
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(4, [])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

        # Test Edge Cases: count_nested_headers == 0
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(0, [])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

        # Test Edge Cases: count_nested_headers is equal to the rows in the table
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(14, [])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

        # Test Edge Cases: count_nested_headers is greater than the rows in the table
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(20, [])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

        # Test Edge Cases: The length of the header_indexes_to_ignore array is greater than count_nested_headers
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(2, [1, 2, 3])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

        # Test Edge Cases: The length of the header_indexes_to_ignore array is greater than the rows in the table
        qc_row_count, original_col_count, row_count, col_count = self.flatten_headers_check_size(
            35, [1, 2, 3, 4, 5, 6, 7, 8, 9, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28])

        # Assert the test results are the same size as expected
        self.assertEqual(row_count, qc_row_count)  # 10 data rows and 1 nested header row
        self.assertEqual(col_count, original_col_count)  # 10 data rows and 1 nested header row

    def test_qc_summary_fields(self):
        """
        Load a delta table and check that the summary of the data is correct.
        
        Asserts that each of the qc fields in the summary dataframe have expected results. 
        QC fields include:
        UniqueValues, Sum, NegativeValues, PositiveValues, TotalCount, NullCount

        Edge Cases:
        TODO: Generate applicable edge cases if necessary
        """
        # Set default test values
        sum_value1 = None
        sum_value2 = None
        sum_value3 = None
        sum_value4 = None
        sum_value5 = None
        sum_value6 = None

        # Set the test schema name and table name
        table_name = self.summary_fields_table_name
        full_table_name = f"{self.schema_name}.{table_name}"

        # Check if the table exists, if so, generate summary results
        if spark.catalog.tableExists(full_table_name):
            df = sql(f"SELECT * FROM {full_table_name}")

            if df.count() == 0:
                print(f"No records found in {full_table_name}")
            else:
                # Accumulate the testing values for each summary field
                summary_df = self.quality_control.generate_summary_table(df, table_name)
                sum_value1 = summary_df.agg(sum("UniqueValues")).collect()[0][0]  # should = 14
                sum_value2 = summary_df.agg(sum("Sum")).collect()[0][0]  # should = 641610.9
                sum_value3 = summary_df.agg(sum("NegativeCount")).collect()[0][0]  # should = 0
                sum_value4 = summary_df.agg(sum("PositiveCount")).collect()[0][0]  # should = 40
                sum_value5 = summary_df.agg(sum("TotalCount")).collect()[0][0]  # should = 100
                sum_value6 = summary_df.agg(sum("NullCount")).collect()[0][0]  # should = 2
        else:
            print(f"Table {full_table_name} does not exist")

        self.assertEqual(sum_value1, 14)  # check UniqueValues summary field is correct
        self.assertEqual(sum_value2, 641610.9)  # check Sum summary field is correct
        self.assertEqual(sum_value3, 0)  # check NegativeCount summary field is correct
        self.assertEqual(sum_value4, 40)  # check PositiveCount summary field is correct
        self.assertEqual(sum_value5, 100)  # check TotalCount summary field is correct
        self.assertEqual(sum_value6, 2)  # check NullCount summary field is correct

    def test_qc_table_comparison(self):
        """
        Check that the resulting quality control summary output is the expected size

        Asserts that the comparison checks output is the correct result when tables are the same or different. 
        Comparison Checks include:
        all_match, schema_match, row_match, data_match
        
        Edge Cases:
        TODO: Generate applicable edge cases if necessary
        """
        # Get test suite of test cases to run comprison checks
        test_suite = self.compare_tables_test_test_suite

        # Set the test control and input schema and table names
        control_table_name = self.compare_tables_control_table_name
        full_table1_name = f"{self.schema_name}.{control_table_name}"

        for test_input in test_suite:
            expected_results = test_suite[test_input]
            print(f"Running sub-test case with input {test_input}")
            # Set default test values
            schema_match = None
            row_match = None
            data_match = None
            all_match = None

            input_table_name = test_input
            full_table2_name = f"{self.schema_name}.{input_table_name}"

            # Check if the table exists, if so, generate summary results
            if (spark.catalog.tableExists(full_table1_name)) and (spark.catalog.tableExists(full_table2_name)):
                table1 = sql(f"SELECT * FROM {full_table1_name}")
                table2 = sql(f"SELECT * FROM {full_table2_name}")
                table1_count = table1.count()
                table2_count = table2.count()

                # Handle case of empty table
                if (table1_count == 0) or (table2_count == 0):
                    print(f"No records found in one of the input tables: {full_table1_name} : count({table1_count}), {full_table2_name} : count({table2_count})")
                else:
                    # Accumulate the testing values for each summary field
                    results = self.quality_control.compare_tables(table1, control_table_name, table2, input_table_name)

                    # Extract results as boolean value
                    schema_match = results['schemas_match']  # should = True
                    row_match = results['rows_match']  # should = True
                    data_match = results['data_match']  # should = True
                    all_match = results['all_match']  # should = True
            else:
                print(f"Table {input_table_name} does not exist")

            self.assertEqual(schema_match, expected_results['schemas_match'])  # check schemas match
            self.assertEqual(row_match, expected_results['rows_match'])  # check rows match
            self.assertEqual(data_match, expected_results['data_match'])  # check data matches
            self.assertEqual(all_match, expected_results['all_match'])  # check PositiveCount summary field is correct


test_runner = unittest.main(argv=[''], exit=False)
test_runner.result.printErrors()

if not test_runner.result.wasSuccessful():
    raise Exception(f"{len(test_runner.result.failures)} of {test_runner.result.testsRun} tests failed.")
