from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.fr.hymaia.exo2.clean.spark_clean_job import add_departement_column
from src.fr.hymaia.exo2.clean.spark_clean_job import filterPlus18 
from src.fr.hymaia.exo2.clean.spark_clean_job import joinDf 
from src.fr.hymaia.exo2.clean.spark_clean_job import code_postal_to_departement 

class SimpleSQLTest(unittest.TestCase):

    spark = spark

    def test_filterPlus18(self):
        # Given
        data = [Row(name="Alice", age=20), Row(name="Bob", age=17)]
        df = self.spark.createDataFrame(data)

        # When
        result_df = filterPlus18(df)

        # Then
        expected_data = [Row(naame="Alice", age=20)]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_joinDf(self):
        # Given
        clients_data = [Row(id=1, zip="12345"), Row(id=2, zip="23456")]
        cities_data = [Row(zip="12345", city="CityA"), Row(zip="23456", city="CityB")]
        clients_df = self.spark.createDataFrame(clients_data)
        cities_df = self.spark.createDataFrame(cities_data)

        # When
        result_df = joinDf(clients_df, cities_df, "zip")

        # Then
        expected_data = [Row(zip="12345", id=1, city="CityA"), Row(zip="23456", id=2, city="CityB")]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_add_departement_column(self):
        # Given
        data = [Row(zip="20190"), Row(zip="20200"), Row(zip="05001")]
        df = self.spark.createDataFrame(data)

        # When
        result_df = add_departement_column(df)

        # Then
        expected_data = [Row(zip="20190", departement="2A"), Row(zip="20200", departement="2B"), Row(zip="05001", departement="05")]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_code_postal_to_departement(self):
        # Test for Corsica 2A
        self.assertEqual(code_postal_to_departement("20100"), "2A")
        # Test for Corsica 2B
        self.assertEqual(code_postal_to_departement("20191"), "2B")
        # Test for other departments
        self.assertEqual(code_postal_to_departement("75001"), "75")
        self.assertEqual(code_postal_to_departement("33000"), "33")
        # Test for edge case at the boundary of 2B
        self.assertEqual(code_postal_to_departement("20200"), "2B")
        # Test for the lowest boundary to ensure it's 2A
        self.assertEqual(code_postal_to_departement("20000"), "2A")

    def test_code_postal_invalid_input(self):
        with self.assertRaises(ValueError):
            code_postal_to_departement("invalide")

   
    def test_integration(self):
        # Préparation des données
        city_data = [("10001", "New York"), ("90001", "Los Angeles")]
        city_df = self.spark.createDataFrame(city_data, ["zip", "city"])
        client_data = [("Alice", 21, "10001"), ("Bob", 12, "10001"), ("Charlie", 42, "90001")]
        client_df = self.spark.createDataFrame(client_data, ["name", "age", "zip"])
        
        # Exécution de la fonctionnalité
        filtered_clients = filterPlus18(client_df)
        joined_df = joinDf(filtered_clients, city_df, "zip")
        result_df = add_departement_column(joined_df)
        
        # Vérification des résultats
        expected_data = [( "10001", "Alice", 21, "New York", "10"), ("90001", "Charlie", 42, "Los Angeles", "90")]
        expected_df = self.spark.createDataFrame(expected_data, ["zip", "name", "age", "city", "departement"])
        
        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))


if __name__ == "__main__":
    unittest.main()
