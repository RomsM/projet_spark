import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import pyspark.sql.functions as f 
from pyspark.sql.window import Window

spark = SparkSession.builder \
        .appName("no_udf") \
        .master("local[*]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def add_category_name_without_udf(df):
    return df.withColumn("category_name", when(df["category"] < 6, "food").otherwise("furniture"))


def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("date","category")
    df = df.withColumn("totalprice_per_category_per_day",
                       f.sum("price").over(window_spec))
    return df

def calculate_total_price_per_category_per_day_last_30_days(df):
    window_spec = Window.partitionBy("date","category").orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum(
        "price").over(window_spec))
    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days") 

def main():
    
    # Mesurer le temps de début
    start_time = time.time()
    
    # Chargement des données
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/resources/exo4/sell.csv")

    # Application de la fonction sans UDF
    df_with_category_name = add_category_name_without_udf(df)
    #df_with_category_name.groupby('category_name').count()
    #df_sorted = df_with_category_name.orderBy(f.desc("price"), f.asc("category_name"))
    print(f"Execution time: {time.time() - start_time} seconds")
    

    df = df.withColumn('category_name', f.when(df['category'] < 6, "food").otherwise("furniture"))
    #df.show()
    df_total_price_per_day = calculate_total_price_per_category_per_day(df)
    #df_total_price_per_day.show()
    df_last_30_days = calculate_total_price_per_category_per_day_last_30_days(df_total_price_per_day)
    #df_last_30_days.show()
    

if __name__ == "__main__":
    main()