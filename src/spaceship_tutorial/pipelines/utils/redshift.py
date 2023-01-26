"""
Author: Christopher Michael-Stokes
Date: 2022-06-29
Company: Code World Wide
"""

from typing import List, Union

from redshift_connector import Connection, connect
import pandas as pd


class Redshift():
    """Connector class for operations on redshift
    """

    def __init__(self, host: str, database: str, port: int, user: str, password: str):
        self.host = host
        self.database = database
        self.port = port
        self.user = user
        self.password = password

    def _init_connection(self) -> Connection:
        """Create and authorise a connection to the DB server.

        Returns:
            Connection: pep-249 compliant connection object
        """
        connection = connect(
            host=self.host,
            database=self.database,
            port=self.port,
            user=self.user,
            password=self.password)

        connection.rollback()
        connection.autocommit = True

        return connection

    def execute_statement(self, statement: str) -> Union[pd.DataFrame, None]:
        """Execute a single sql statement and fetch the results as a dataframe.

        Args:
            statement (str): A containing a single DDL or DQL statement.

        Returns:
            Union[pd.DataFrame, None]: If any, dataframe of result from the query, else None.
        """
        connection = self._init_connection()

        with connection.cursor() as cursor:
            cursor.execute(statement)

            if cursor.rowcount != -1:
                return cursor.fetch_dataframe()

    def execute_multiple(self, statements: List[str]) -> List[Union[pd.DataFrame, None]]:
        """Execute multiple sql statements and fetch the results

        Args:
            statements (List[str]): A list of individual DDL or DQL statements.

        Returns:
            List[Union[pd.DataFrame, None]]: A list containing returns from each query.
            If a query has a return, the list item will be a pandas DataFrame, else None
        """
        connection = self._init_connection()

        results: List[Union[pd.DataFrame, None]] = []

        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

                if cursor.description is not None and cursor.description != []:
                    results.append(cursor.fetch_dataframe())
                else:
                    results.append(None)

        return results