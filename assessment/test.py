import sqlalchemy
import run
import pandas as pd

# connect to the database
engine = sqlalchemy.create_engine("mysql://datatest:alligator@database/datatestdb")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)

# clean out table from previous runs
connection.execute(Example.delete())

names = ["Joe", "Mary", "Sue", "Bill"]

for name in names:
    connection.execute(Example.insert().values(name = name))

rows = connection.execute(sqlalchemy.sql.select([Example])).fetchall()
print("Found rows in database: ", len(rows))

assert len(rows) == len(names)

print("Test Successful")


# Unit tests
def test_unify_phone_separator():
    INPUT_VALUE = "111.111.1111"
    RESULT_VALUE = "111-111-1111"

    df_mock = pd.DataFrame({"PhoneNumber": [INPUT_VALUE]})
    run.unify_phone_separator(df_mock)
    if df_mock["PhoneNumber"][0] != RESULT_VALUE:
        raise Exception("test_unify_phone_separator failed")


def test_strip_interests():
    INPUT_VALUE = "  Some interest "
    RESULT_VALUE = "Some interest"

    df_mock = pd.DataFrame({"Interest1": [INPUT_VALUE]})
    run.strip_interests(df_mock)
    if df_mock["Interest1"][0] != RESULT_VALUE:
        raise Exception("test_strip_interests failed")


test_unify_phone_separator()
test_strip_interests()
print("Unit tests passed successfully")
