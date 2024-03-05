#En gros dans cet exo ca créé des partitions sous le nom count et dans le dossier count(1) il y aura tous les mots qui sont comptés une seule fois

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
    .appName("wordcount") \
    .master("local[*]") \
    .getOrCreate()

    # Lire le fichier src/resources/exo1/data.csv en tant que DataFrame
    input_df = spark.read.option("header", "true").csv("/home/romain/repo_spark/spark-handson/src/resources/exo1/data.csv")

     # Appliquer la fonction wordcount avec le bon nom de colonne
    result_df = wordcount(input_df, 'text')  # Assurez-vous de spécifier le bon nom de colonne ici

    # Écrire le résultat dans un fichier Parquet partitionné par "count"
    result_df.write.partitionBy('count').mode("overwrite").parquet("data/exo1/output") #si le dossier existe déja on ecrase pour réecrire on aurait aussi pu utiliser .mode(overwrite)

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
