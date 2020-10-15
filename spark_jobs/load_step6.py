import sys
from pyspark.sql import SQLContext, SparkSession
from datetime import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('step6 table extract').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

sqlCtx = SQLContext(spark)


def load_table(table):
    df = sqlCtx.read.format("jdbc").options(
        url="jdbc:sqlite:/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/data/sample_db.sqlite",
        driver="org.sqlite.JDBC",
        dbtable=table
    ).load()

    return df


def table_to_parquet(df):
    file_suffix = datetime.today().strftime('%Y-%m-%d')
    df.write.parquet(
        '/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/output/step6-{}'.format(
            file_suffix
        ))


def main():
    table_to_parquet(load_table("step6"))


if __name__ == '__main__':
    main()
