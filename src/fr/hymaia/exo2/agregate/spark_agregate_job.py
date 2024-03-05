import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main() : 
    spark = SparkSession.builder \
        .appName("AggregateJob") \
        .master("local[*]") \
        .getOrCreate()
    
    calculate_population_by_department(spark, "data/exo2/clean", "data/exo2/aggregate")
    
    spark.stop()

def calculate_population_by_department(spark, input_path, output_path):
    # Lire le fichier d'entrée
    input_df = spark.read.parquet(input_path)

    # Calculer la population par département
    result_df = input_df.groupBy("departement").agg(count("name").alias("nb_people")) \
        .orderBy(["nb_people", "departement"], ascending=[False, True])
    
    # Écrire le résultat dans un fichier CSV
    write_output_csv(result_df, output_path)

def write_output_csv(df, output_path):
    df.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    main()