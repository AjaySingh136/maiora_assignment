# tests/api_test.py

import unittest
import requests

class TestJokeAPI(unittest.TestCase):
    def test_fetch_jokes(self):
        response = requests.get('http://127.0.0.1:5000/fetch-jokes')
        self.assertEqual(response.status_code, 200)

if __name__ == '__main__':
    unittest.main()
