from pyspark.sql import SparkSession
import json
import vertica_python
import os

def db_connection():
    conn_info = {'host':'','port':'','user':'','password':''}
    db = vertca_python.connect(**conn_info)
    cur = db.cursor()
    return cur, db

def data_copy(filename, table_name):
    copy_cmd = """
    COPY {0} FROM {1} PARSER fcsvparser(type='traditional', delimiter=',');
    """
    cur.execute(copy_cmd.format(table_name, filename))
    db.commit()

spark = SparkSession.builder().master("local[1]").appName("pyspark_vertica").getOrCreate()
file_list = ['products.csv']
schema = StructType() \
      .add("name",StringType(),True) \
      .add("sku",StringType(),True) \
      .add("description",StringType(),True)

cur, db = db_connection()

for i in file_list:
    stage_feed_csv = spark.read.csv(i, schema = schema)
    stage_feed_csv.createOrReplace("stage")
    table = 'sku_details'
    df = spark.sql("""
    select 
    name,
    sku,
    description
    from stage
    /* business logic*/
    """)
    df.write.format('com.databricks.spark.csv').save('transformred_file.csv')
    data_copy(filename = 'transformred_file.csv', table_name = table)