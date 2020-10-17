# jerry-coding-challenge

To run this solution on your local, you would need a Spark cluster and Apache Airflow. A bare-metals SQLite DB is included as a OLTP backend.

## Airflow setup

- Go to Admin -> Connections
-- Add a connection with Conn Id `spark_default` and make sure the connection type is Spark
-- Assign appropriate host and port
-- Add this JSON in extra: `{"queue": "root.default", "spark-home": "<absolute-path-to-spark>"}`

Point Airflow DAG directory to the DAG directory of the repository. Airflow should automatically pickup the DAGs and start running them once they are enabled.
