from pyspark.sql.functions import col, explode_outer, to_date
from pyspark.sql import SparkSession


def explode_schema(data):
    columns = []
    struct_array_exists = 0
    explode_exists = False

    def find_new_schema(data, stack=[]):

        nonlocal struct_array_exists
        nonlocal explode_exists
        if isinstance(data["type"], dict):
            if "name" in data:
                stack.append(data["name"])
            find_new_schema(data["type"], stack.copy())

        elif data["type"] == "struct":
            struct_array_exists += 1
            for field in data["fields"]:
                find_new_schema(field, stack.copy())

        elif data["type"] == "array":
            struct_array_exists += 1
            if explode_exists:
                columns.append(col(".".join(stack)).alias("_".join(stack)))
            else:
                explode_exists = True
                columns.append(explode_outer(".".join(stack)).alias("_".join(stack)))
        else:
            stack.append(data["name"])
            columns.append(col(".".join(stack)).alias("_".join(stack)))

    find_new_schema(data)
    return columns, struct_array_exists


spark = SparkSession.builder \
    .master("local[10]") \
    .appName("json_to_parquet") \
    .getOrCreate()

df = spark.read.json("json_file.json")

columns, exists = explode_schema(df.schema.jsonValue())
while exists > 1:
    # print(exists, df.schema.jsonValue())
    df = df.select(*columns)
    columns, exists = explode_schema(df.schema.jsonValue())

final_df = df.withColumn("rpt_dt", to_date(col("context_timestamp")))
final_df.write.mode("overwrite").parquet("output_bucket")

