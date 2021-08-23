class DataQualityChecks:
    
    @staticmethod
    def has_rows(db_hook, table):
        """
        Returns 'True' if table has at least one row.   
        
        Parameters
        ----------
            db_hook (PostgresHook) - name of the hook used to connect to database
            table (str) - the table in question
        """
        check_query = 'SELECT COUNT(*) FROM {};'.format(table)
        query_result = db_hook.get_records(check_query)[0][0]

        return query_result > 0
        
    @staticmethod
    def row_count_between(db_hook, table, lower_bound, upper_bound):
        """
        Returns 'True' if table row count is between two numbers.   
        
        Parameters
        ----------
            db_hook (PostgresHook) - name of the hook used to connect to database
            table (str) - the table in question
            lower_bound (int) - smallest number of rows table should have
            upper_bound (int) - largest number of rows table should have
        """
        check_query = 'SELECT COUNT (*) FROM {};'.format(table)
        query_result = db_hook.get_records(check_query)[0][0]

        return lower_bound <= query_result <= upper_bound

    @staticmethod
    def no_nulls(db_hook, table, column):
        """
        Returns 'True' if a column does not contain any NULL values.
        
        Parameters
        ----------
            db_hook (PostgresHook) - name of the hook used to connect to database
            table (str) - the table containing the column in question
            column (str) - the column to check
        """
        check_query = 'SELECT COUNT (*) FROM {} WHERE {} IS NULL;'.format(table, column)
        query_result = db_hook.get_records(check_query)[0][0]

        return query_result == 0

    @staticmethod
    def all_distinct(db_hook, table, column):
        """
        Returns 'True' if all values in a column are distinct.   
        
        Parameters
        ----------
            db_hook (PostgresHook) - name of the hook used to connect to database
            table (str) - the table containing the column in question
            column (str) - the column to check
        """
        distinct_query = 'SELECT COUNT (DISTINCT {}) FROM {};'.format(column, table)
        distanct_value_count = db_hook.get_records(distinct_query)[0][0]

        total_query = 'SELECT COUNT ({}) FROM {};'.format(column, table)
        total_value_count = db_hook.get_records(total_query)[0][0]

        return distanct_value_count == total_value_count


    mapping_dict = {
        'has_rows': has_rows.__func__,
        'row_count_between': row_count_between.__func__,
        'no_nulls': no_nulls.__func__,
        'all_distinct': all_distinct.__func__,
    }

