import unittest
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch
from datetime import datetime
from io import StringIO
import psycopg2
from process_transaction_data import CSVToDB, User, Transaction \

class TestCSVToDB(unittest.TestCase):

    def setUp(self):
        # Setup a connection to a mock database
        self.mock_conn = MagicMock(spec=psycopg2.extensions.connection)
        self.mock_cursor = MagicMock(spec=psycopg2.extensions.cursor)
        self.mock_conn.cursor.return_value = self.mock_cursor

    def test_ensure_tables_exist(self):
        self.mock_conn.commit = MagicMock()
        csv_to_db = CSVToDB(self.mock_conn, 'transaction', Transaction)
        csv_to_db.ensure_tables_exist()

        # Check if the SQL commands are executed
        # self.assertTrue(self.mock_cursor.execute.called)
        self.assertTrue(self.mock_conn.commit.called)
    
    # @patch('process_transaction_data.datetime')  
    def test_ensure_tables_exist_cursor_is_excecuted(self):
        self.mock_conn.commit = MagicMock()
        csv_to_db = CSVToDB(self.mock_conn, 'transaction', Transaction)
        csv_to_db.ensure_tables_exist()

        # Check if the SQL commands are executed
        self.assertTrue(self.mock_cursor.execute.called)
        

    @patch('process_transaction_data.datetime') 
    def test_insert_user_is_successful(self, mock_datetime):
        fake_now = datetime(2021, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = fake_now

        csv_to_db = CSVToDB(self.mock_conn, 'transaction', Transaction)

        mock_transaction = MagicMock(spec=Transaction)
        mock_transaction.agentPhoneNumber = '1234567890'

        # Run the method
        csv_to_db.insert_user(mock_transaction)

        self.mock_cursor.execute.assert_called_with("""
            INSERT INTO "user" ("phoneNumber", "createdAt", "updatedAt", "nTransactions")
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("phoneNumber")
            DO NOTHING;
        """, (mock_transaction.agentPhoneNumber, fake_now, fake_now, 0))

        self.assertTrue(self.mock_conn.commit.called)
 