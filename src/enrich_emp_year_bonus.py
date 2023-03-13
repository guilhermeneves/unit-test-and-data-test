from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def enrich_employee_year_end_bonus(df: DataFrame, salary_col: str) -> DataFrame:
    """Enrich data by adding a column year_end_bonus based on the following formula:
    - (Salary * 2) / 5 if the employee earns GREATER THAN OR EQUAL TO 50,000
    - (Salary * 3) / 4 if the employee earns LESS THAN 50,000
    """
    bonus_formula_emp_more_50k = (col(salary_col) * 2) / 5
    bonus_formula_emp_less_50k = (col(salary_col) * 3) / 4

    df_enriched = df.withColumn(
        "year_end_bonus",
        when(col(salary_col) >= 50000, bonus_formula_emp_more_50k).otherwise(
            bonus_formula_emp_less_50k
        ),
    )

    return df_enriched
