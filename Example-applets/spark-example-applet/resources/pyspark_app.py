import pyspark
import sys

spark = pyspark.sql.SparkSession.builder.enableHiveSupport().getOrCreate()

sql = sys.argv[1].strip(';\n')
field_name = sys.argv[2]

df = spark.sql(sql)
pdf = df.toPandas()
mean = pdf[f'participant.{field_name}'].mean()

with open('temp_file.txt', 'w') as f:
    f.write(str(mean))