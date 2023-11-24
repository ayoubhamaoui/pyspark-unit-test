from src.transform import *


def test_transform_1(spark_session):
    # Frame input parameter to function to be tested
    fact_df = spark_session.read.csv(
            "tests/resources/input/orders_data.csv", header=True, inferSchema=True
        )
    lookup_df = spark_session.read.csv(
        "tests/resources/input/citytier_pincode.csv", header=True, inferSchema=True
    )

    # Call the actual function that we want to test
    df_actual = transform_1(fact_df, lookup_df)

    # Reading the expected data
    expected_df = spark_session.read.csv(
        "tests/resources/expected/fact_joined_with_lookup.csv", header=True, inferSchema=True
    )

    # checking if the dataframe returned from the function is same as expected
    assert sorted(expected_df.collect()) == sorted(df_actual.collect())


def test_transform_2(spark_session):
    # Frame input parameter to function to be tested
    fact_df = spark_session.read.csv(
            "tests/resources/input/orders_data.csv", header=True, inferSchema=True
        )

    # Call the actual function that we want to test
    df_actual = transform_2(fact_df)

    df_actual_c7 = df_actual.where("count == 7")
    df_actual_c28 = df_actual.where("count == 28")



    # checking if the dataframe returned from the function is same as expected
    assert df_actual_c28.count() == 6
    assert df_actual_c7.count() == 44
