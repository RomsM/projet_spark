import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# Assumant que spark est déjà initialisé correctement
spark = SparkSession.builder.appName("PythonUDFExample").getOrCreate()

csv_dir_path = "src/resources/exo4/sell.csv"

def category_name(category):
    if category < 6:
        return "food"
    else:
        return "furniture"

category_name_udf = udf(category_name, StringType())

def main():
    # Mesurer le temps de début
    start_time = time.time()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_dir_path)
    #df.show()
    df_with_category_name = df.withColumn("category_name", category_name_udf(df["category"]))
    #df_with_category_name.show()
    #df_sorted = df_with_category_name.orderBy(F.desc("price"), F.asc("category_name"))
    #df_with_category_name.groupby('category_name').count()
    # Mesurer et afficher le temps de fin
    print(f"Execution time: {time.time() - start_time} seconds")

if __name__ == "__main__":
    main()
