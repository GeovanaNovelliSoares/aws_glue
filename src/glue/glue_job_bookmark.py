import sys
from awsglue.transforms import *  # type: ignore
from awsglue.utils import getResolvedOptions  # type: ignore
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext(master="local[*]")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

csv_source_path = "/home/glue_user/workspace/sample_data/retail_db/order_items/"
json_source_path = "/home/glue_user/workspace/sample_data/retail_db_json/order_items/"
csv_output_path = "/home/glue_user/workspace/output/csv_output/"
json_output_path = "/home/glue_user/workspace/output/json_output/"
unified_output_path = "/home/glue_user/workspace/output/unified_output/"
df_csv = spark.read.option("header", "true").csv(csv_source_path)

df_json = spark.read.json(json_source_path)

print(f"Registros CSV lidos: {df_csv.count()}")
df_csv.show()

print(f"Registros JSON lidos: {df_json.count()}")
df_json.show()

df_csv.write.mode("overwrite").option("header", "true").csv(csv_output_path)
print(f"Arquivo CSV salvo em: {csv_output_path}")
df_json.write.mode("overwrite").json(json_output_path)
print(f"Arquivo JSON salvo em: {json_output_path}")
csv_dynamic_frame = DynamicFrame.fromDF(df_csv, glueContext, "csv_dynamic_frame")
json_dynamic_frame = DynamicFrame.fromDF(df_json, glueContext, "json_dynamic_frame")
try:
    df_unified = df_csv.unionByName(df_json, allowMissingColumns=True)

    unified_dynamic_frame = DynamicFrame.fromDF(df_unified, glueContext, "unified_dynamic_frame")

    print(f"Registros unificados: {df_unified.count()}")
    unified_dynamic_frame.show()
    df_unified.write.mode("overwrite").option("header", "true").csv(unified_output_path)
    print(f"Arquivo unificado CSV salvo em: {unified_output_path}")

except Exception as e:
    print(f"❌ Não foi possível unir os DataFrames: {e}")

job.commit()
