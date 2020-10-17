import sys
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import lit, col, udf
from pyspark.sql import types
from datetime import datetime


assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('step1 dimension').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

sqlCtx = SQLContext(spark)

string_to_date = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), types.DateType())


def generate_table():
    users = spark.read.parquet("/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/datalake/users-2020-10-16/")
    step1 = spark.read.parquet("/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/datalake/step1-2020-10-16")

    users = users.withColumn('date', string_to_date(col('date')))

    users.createOrReplaceTempView("users")
    step1.createOrReplaceTempView("step1")

    step1_fact = spark.sql(
        """
        SELECT users.user_id FROM
        users, step1
        WHERE users.user_id = step1.user_id
        """
    )
    step1_fact = step1_fact.withColumn("visited", lit(1))

    file_suffix = datetime.today().strftime('%Y-%m-%d')
    step1_fact.write.parquet(
        '/Users/aleemr/powerhouse/interviews/'
        'jerry-coding-challenge/spark_jobs/warehouse/step1_fact_table-{}'.format(file_suffix))


def main():
    generate_table()


if __name__ == '__main__':
    main()
