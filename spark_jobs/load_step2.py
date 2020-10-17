import sys
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import types
from datetime import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('step2 table extract').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

sqlCtx = SQLContext(spark)


def step_schema():
    return types.StructType([
        types.StructField('user_id', types.IntegerType(), False),
        types.StructField('page', types.StringType(), False)
    ])


def load_table(table, schema):
    df = sqlCtx.read.format("jdbc").options(
        url="jdbc:sqlite:/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/data/sample_db.sqlite",
        driver="org.sqlite.JDBC",
        dbtable=table,
        schema=schema
    ).load()

    return df


def table_to_parquet(df):
    file_suffix = datetime.today().strftime('%Y-%m-%d')
    df.write.parquet(
        '/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/datalake/step2-{}'.format(
            file_suffix
        ))


def main():
    table_to_parquet(load_table("step2", step_schema()))


if __name__ == '__main__':
    main()
