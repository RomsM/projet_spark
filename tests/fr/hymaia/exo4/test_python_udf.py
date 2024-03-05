from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo4.python_udf import category_name

class test_python_udf(unittest.TestCase):

    spark = spark

    def test_category_name(self):
        self.assertEqual(category_name(5), "food")
        self.assertEqual(category_name(6), "furniture")

if __name__ == '__main__':
    unittest.main()