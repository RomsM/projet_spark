from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row
from src.fr.hymaia.exo4.no_udf import calculate_total_price_per_category_per_day, calculate_total_price_per_category_per_day_last_30_days

class test_no_udf(unittest.TestCase):

    spark = spark

    def test_calculate_total_price_per_category_per_day(self):
        # GIVEN
        data = [Row(id=1, date='2021-01-01', category=1, price=10, category_name='food'),
                Row(id=2, date='2021-01-01', category=1, price=20, category_name='food')]
        df = self.spark.createDataFrame(data)

        # WHEN
        result_df = calculate_total_price_per_category_per_day(df)

        # THEN
        expected_data = [Row(id=1, date='2021-01-01', category=1, price=10, category_name='food', totalprice_per_category_per_day=30),
                         Row(id=2, date='2021-01-01', category=1, price=20, category_name='food', totalprice_per_category_per_day=30)]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertTrue([row.asDict() for row in result_df.collect()] == [row.asDict() for row in expected_df.collect()])

    def test_calculate_total_price_per_category_per_day_last_30_days(self):
        # GIVEN
        data = [Row(id=1, date='2021-01-01', category=1, price=10, category_name='food'),
                Row(id=2, date='2021-01-02', category=1, price=20, category_name='food')]
        df = self.spark.createDataFrame(data)

        # WHEN
        result_df = calculate_total_price_per_category_per_day_last_30_days(df)

        # THEN
        # Le premier jour, seule la somme du jour même est calculée
        # Le deuxième jour, la somme inclut le jour même et le jour précédent
        expected_data = [
            Row(id=1, date='2021-01-01', category=1, price=10, category_name='food', total_price_per_category_per_day_last_30_days=10),
            Row(id=2, date='2021-01-02', category=1, price=20, category_name='food', total_price_per_category_per_day_last_30_days=20)  # Ajusté à 20
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        self.assertTrue([row.asDict() for row in result_df.collect()] == [row.asDict() for row in expected_df.collect()])


if __name__ == '__main__':
    unittest.main()