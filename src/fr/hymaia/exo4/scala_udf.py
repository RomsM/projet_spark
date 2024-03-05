import time
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
from pyspark.sql import functions as F

# Initialisation de SparkSession avec le JAR contenant l'UDF Scala
spark = SparkSession.builder \
    .appName("ScalaUDFExample") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()



def addCategoryName(col):
    sc = spark.sparkContext
    # Accès à l'UDF Scala via le SparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # Application de l'UDF Scala à la colonne spécifiée et retour d'une colonne PySpark
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    # Mesurer le temps de début
    start_time = time.time()
    # Supposons que df est votre DataFrame initial
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/resources/exo4/sell.csv")
    #df.show()

    # Application de l'UDF pour ajouter la colonne category_name
    df_with_category_name = df.withColumn("category_name", addCategoryName(df["category"]))
    #df_with_category_name.show()
    #df_with_category_name.groupby('category_name').count()
    #df_sorted = df_with_category_name.orderBy(F.desc("price"), F.asc("category_name"))
    # Mesurer et afficher le temps de fin
    print(f"Execution time: {time.time() - start_time} seconds")

if __name__ == "__main__":
    main()


