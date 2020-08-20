import pandas as pd
from pandas import ExcelWriter

def filter_pandas_dataframe_fields_by_regex(self, regex):
    return self[self.apply(lambda row: row.astype(str).str.contains(regex).any(), axis=1)]


def filter_pandas_dataframe_field_by_regex(self, column_name, regex):
    return self[self[column_name].str.contains(regex)]


pd.DataFrame.filter_by_regex = filter_pandas_dataframe_fields_by_regex
pd.DataFrame.filter_column_by_regex = filter_pandas_dataframe_field_by_regex

def save_dataframes_to_excel(dfs, sheet_names, output_file):
    print("Saving dataframes to excel: "+output_file)
    writer = ExcelWriter(output_file,  engine='xlsxwriter')
    workbook = writer.book

    def get_col_widths(df):
        # First we find the maximum length of the index column
        idx_max = max([len(str(s)) for s in df.index.values] + [len(str(df.index.name))])
        # Then, we concatenate this to the max of the lengths of column name and its values for each column, left to right
        return [idx_max] + [max([len(str(s)) for s in df[col].values] + [len(col)]) for col in
                            df.columns]

    for n, df in enumerate(dfs):
        if not isinstance(df, pd.DataFrame):
            continue
        df.to_excel(writer, sheet_names[n])
        worksheet = writer.sheets[sheet_names[n]]
        for i, width in enumerate(get_col_widths(df)):
            worksheet.set_column(i, i, min(width,100))

        worksheet.add_table(0, 0, len(df.index), len(df.columns), {'columns': [{'header':'Idx'}] + [  {'header': c } for c in list(df)   ]})

    writer.save()


def create_pandas_dataframe(data, columns):
    import pandas
    d = pandas.DataFrame(data)
    d = d.rename(columns=dict([(i,c) for i,c in enumerate(columns)]))
    return d

