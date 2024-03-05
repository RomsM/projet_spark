from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql.functions import col
from src.fr.hymaia.exo4.scala_udf import addCategoryName

class test_scala_udf(unittest.TestCase):

    spark = spark

    def test_addCategoryName(self):
        

        # Créer un DataFrame de test
        data = [("0", "2019-02-17", 6, 40.0),
                ("1", "2015-10-01", 4, 69.0)]
        df = self.spark.createDataFrame(data, ["id", "date", "category", "price"])

        # Appliquer la fonction (simulée) addCategoryName
        df_with_category_name = df.withColumn("category_name", addCategoryName(col("category")))

        # Vérifier que la colonne "category_name" est bien ajoutée
        self.assertIn("category_name", df_with_category_name.columns)

        # Vérifier le contenu de la colonne (exercice laissé pour plus de complexité si nécessaire)

if __name__ == '__main__':
    unittest.main()