from spark_pyspark.etl_instacart import spark_session
def test_spark():
    assert spark_session("unit-test", 1) is not None
