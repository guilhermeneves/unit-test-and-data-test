# Unit and Data Testing

## Unit Tests
Suppose you have a function that receives a dataframe with employees salaries column `salary` and the function needs to return the calculated column `year_end_bonus`
 
 We can test if the formula is valid based on some mock data inputs with some scenarios like happy path and also invalid scenarios and also boundaries scenarios by Creating the Unit testing using pytest and chispa

See: `tests/test_enrich_emp_year_bonus.py`

Installing dev dependencies:

**Be sure to have Java Runtime before install dependencies**

```
pipenv install -d
```

Running tests:

```
pipenv run pytest -k enrich_emp_year_bonus 
```

# Data test very basic example

Data testing is suppose to test only the data and not the code or business rules itself, in this case we can test if the pipeline that generates salaries data checks for instance if the `salary`column contains only numbers or if the data is not duplicated based on a primary key like `employee_id`