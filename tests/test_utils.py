import pytest
from mock import Mock, call, patch

from pganonymizer.utils import get_connection, truncate_tables


class TestGetConnection:

    @patch('pganonymizer.utils.psycopg2.connect')
    def test(self, mock_connect):
        connection_data = {
            'dbname': 'test',
            'user': 'user',
            'password': 'password',
            'host': 'localhost',
            'port': 5432
        }
        get_connection(connection_data)
        mock_connect.assert_called_once_with(**connection_data)


class TestTruncateTables:

    @pytest.mark.parametrize('tables, expected_sql', [
        [('table_a', 'table_b'), 'TRUNCATE TABLE table_a, table_b;'],
        [(), None],
    ])
    def test(self, tables, expected_sql):
        mock_cursor = Mock()
        connection = Mock()
        connection.cursor.return_value = mock_cursor
        truncate_tables(connection, tables)
        if tables:
            connection.cursor.assert_called_once()
            assert mock_cursor.execute.call_args_list == [call(expected_sql)]
            mock_cursor.close.assert_called_once()
        else:
            connection.cursor.assert_not_called()
            mock_cursor.execute.assert_not_called()
            mock_cursor.close.assert_not_called()
