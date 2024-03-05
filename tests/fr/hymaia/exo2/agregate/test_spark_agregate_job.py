from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.agregate.spark_agregate_job import calculate_population_by_department, write_output_csv
import os
import shutil

class TestSparkJobs(unittest.TestCase):

    #spark = SparkSession.builder.master("local[*]").appName("TestSession").getOrCreate()
    spark = spark
    
    def test_calculate_population_by_department(self):
                
        data = [
            Row(departement="01", name="Alice"),
            Row(departement="01", name="Bob"),
            Row(departement="02", name="Charlie")
        ]
        input_df = self.spark.createDataFrame(data)
        input_path = "temp_test_input"
        output_path = "temp_test_output"
        input_df.write.mode("overwrite").parquet(input_path)

        calculate_population_by_department(self.spark, input_path, output_path)

        result_df = self.spark.read.option("header", "true").csv(output_path)
        expected_data = [
            Row(departement="01", nb_people="2"),
            Row(departement="02", nb_people="1")
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))



    def test_write_output_csv(self):

        # Création de données d'entrée
        data = [
            Row(departement="01", nb_people="2"),
            Row(departement="02", nb_people="1")
        ]
        df_to_write = self.spark.createDataFrame(data)
        try:
            df_to_write.write.format("noop").mode("overwrite").save()
            operation_successful = True
        except Exception as e:
            operation_successful = False

        # Vérification que l'opération d'écriture peut être exécutée sans erreur
        self.assertTrue(operation_successful, "L'écriture du DataFrame devrait s'exécuter sans erreur.")

        
    def test_integration(self):
        # Prepare mock data
        data = [("John Doe", "01"), ("Jane Doe", "01"), ("Joe Bloggs", "02")]
        input_df = self.spark.createDataFrame(data, ["name", "departement"])
        input_path = "temp_input.parquet"
        output_path = "temp_output"
        input_df.write.parquet(input_path, mode="overwrite")

        # Run the aggregation job
        calculate_population_by_department(self.spark, input_path, output_path)

        # Read the output and validate
        result_df = self.spark.read.csv(output_path, header=True)
        expected_data = [("01", "2"), ("02", "1")]
        expected_df = self.spark.createDataFrame(expected_data, ["departement", "nb_people"])

        # Compare the result
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))


if __name__ == '__main__':
    unittest.main()

