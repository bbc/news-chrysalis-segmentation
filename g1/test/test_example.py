import unittest
from src import example

class TestExample(unittest.TestCase):

    def setUp(self):
        self.a = 1
        self.b = ['a','b','c']
        pass

    def test_a_equal(self):
        self.assertEqual(self.a, 1)

    def test_b_contains(self):
        self.assertIn('a',self.b)

    def test_example_simple(self):
        # Given
        x = 1

        # When
        val = example.simple_function(x)

        # Then
        self.assertEqual(val, x)

    def test_example_simple_flag(self):
        # Given
        x = 1

        # When
        val = example.simple_function(x, some_flag=True)

        # Then
        self.assertEqual(val, -x)

if __name__ == '__main__':
    unittest.main()