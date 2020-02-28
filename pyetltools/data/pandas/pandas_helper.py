import pandas


def filter_pandas_dataframe_fields_by_regex(self, regex):
    return self[self.apply(lambda row: row.astype(str).str.contains(regex).any(), axis=1)]


pandas.DataFrame.filter_by_regex = filter_pandas_dataframe_fields_by_regex
