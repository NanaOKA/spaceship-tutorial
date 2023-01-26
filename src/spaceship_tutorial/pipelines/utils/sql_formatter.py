from typing import List

import sqlparse


class Formatter:
    @staticmethod
    def insert_vars(sql_string: str, **kwargs) -> str:
        """Insert parameters provided by kwargs to input sql string.  Will throw a runtime
        error if any parameters do not exist in the sql string.

        Parameters
        ----------
        sql_string : str
            Single string of valid sql (no validation is performed)

        kwargs : Dict
            Mapping of parameters to values

        Returns
        -------
            Input string with supplied parameters inserted

        """
        for arg, value in kwargs.items():
            assert sql_string.find(arg) != -1, f'\'{arg}\' not found in input'

            sql_string = sql_string.replace(f'$[{arg}]', str(value))

        return sql_string

    @staticmethod
    def split_statements(sql_string: str) -> List[str]:
        """Split sql string into individual statements

        Parameters
        ----------

        sql_string : str
            A single string of valid sql containing 1 or more statements

        Returns
        -------
            The list of sql statements contained in the input string
        """
        return sqlparse.split(sql_string)

    @staticmethod
    def join_lines(sql_lines: List[str]) -> str:
        """Concatenate lines of sql to a single string

        Parameters
        ----------

        sql_lines : List[str]
            A list of sql strings

        Returns
        -------
            A single string of sql
        """
        return '\n'.join(line.strip() for line in sql_lines if not line.isspace() and not line.startswith('-')).strip()
