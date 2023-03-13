from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest
from chispa.dataframe_comparer import assert_df_equality
from src.enrich_emp_year_bonus import enrich_employee_year_end_bonus



@pytest.fixture(scope="class")
def spark():
    spark = (
        SparkSession.builder.master("local[*]").appName("pytest-pyspark").getOrCreate()
    )
    yield spark
    spark.stop()


class TestEnrichEmployeeYearEndBonus:

    def test_only_salaries_greater_50k(self, spark):
        """test input data with only employees with salary greater than 50000"""
        input_data = [("John", 55000.0), ("Jane", 60000.0), ("Bob", 70000.0)]
        input_schema = "name string, salary double"
        input_df = spark.createDataFrame(input_data, input_schema)

        expected_data = [
            ("John", 55000.0, 22000.0),
            ("Jane", 60000.0, 24000.0),
            ("Bob", 70000.0, 28000.0),
        ]
        expected_schema = "name string, salary double, year_end_bonus double"

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        salary_col = "salary"
        result_df = enrich_employee_year_end_bonus(input_df, salary_col)
        assert_df_equality(result_df, expected_df)

    def test_only_employees_salaries_less_50k(self, spark):
        """test input data with only employees with salary less than 50000"""
        input_data = [("John", 20000.0), ("Jane", 30000.0), ("Bob", 40000.0)]
        input_schema = "name string, salary double"
        input_df = spark.createDataFrame(input_data, input_schema)

        expected_data = [
            ("John", 20000.0, 15000.0),
            ("Jane", 30000.0, 22500.0),
            ("Bob", 40000.0, 30000.0),
        ]
        expected_schema = "name string, salary double, year_end_bonus double"

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        salary_col = "salary"
        result_df = enrich_employee_year_end_bonus(input_df, salary_col)
        assert_df_equality(result_df, expected_df)

    def test_only_employees_boundary_50k(self, spark):
        """test input data with employee with salary in the boundary 50000"""
        input_data = [("Julia", 50000.0)]
        input_schema = "name string, salary double"
        input_df = spark.createDataFrame(input_data, input_schema)

        expected_data = [
            ("Julia", 50000.0, 20000.0)
        ]
        expected_schema = "name string, salary double, year_end_bonus double"

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        salary_col = "salary"
        result_df = enrich_employee_year_end_bonus(input_df, salary_col)
        assert_df_equality(result_df, expected_df)
