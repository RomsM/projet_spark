import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder \
        .appName("clean") \
        .master("local[*]") \
        .getOrCreate()

    # Correction de l'option "delimiter" et lecture des fichiers
    city_df = spark.read.option("header", "true").option("delimiter", ",").csv("/home/romain/repo_spark/spark-handson/src/resources/exo2/city_zipcode.csv")
    #on est pas obligé de spécifié le delimiter car la virgule est le delemiter par defaut
    client_df = spark.read.option("header", "true").option("delimiter", ",").csv("/home/romain/repo_spark/spark-handson/src/resources/exo2/clients_bdd.csv") 

    client_df_majeur = filterPlus18(client_df)
    df_join = joinDf(client_df_majeur, city_df, "zip")

    # Convertir la colonne "zip" en chaîne de caractères
    df_join = df_join.withColumn("zip", df_join["zip"].cast(StringType()))
    
    # Application de la UDF
    df_join_dept = add_departement_column(df_join)
    
    # Écrire le résultat dans un fichier Parquet
    df_join_dept.write.parquet("data/exo2/output", mode="overwrite")
    
    test = df_join_dept.groupBy(f.col("departement")).count()
    test.sort("departement").show(150)
    # Arrêter la session Spark
    spark.stop()

def filterPlus18(df):
    return df.where(f.col("age") >= 18)

def joinDf(df1, df2, col):
    return df1.join(df2, col)

# Création de la fonction Python
def code_postal_to_departement(code_postal):
    try:
        int(code_postal)  # Essayer de convertir en entier pour vérifier que c'est bien numérique
        if code_postal <= "20190":
            return "2A"
        elif code_postal <= "20200":
            return "2B"
        else:
            return code_postal[:2]
    except ValueError:  # Si la conversion en entier échoue, on suppose que le code postal est invalide
        raise ValueError("Code postal invalide")
    
#Ajouter uniquement le code départelment à deux chiffres -> nouvelle colonne
def add_departement_column(output_df):
    output_df_with_departement = output_df.withColumn("departement", f.substring("zip", 1, 2))

    #Particularité pour la corse -> définition d'une fonction avec when et otherwise
    output_df_with_departement = output_df_with_departement.withColumn(
        "departement",
        f.when(output_df_with_departement["departement"] == "20", 
             f.when(output_df_with_departement["zip"] <= 20190, "2A").otherwise("2B")
            ).otherwise(output_df_with_departement["departement"])
    )
    return output_df_with_departement 

if __name__ == "__main__":
    main()