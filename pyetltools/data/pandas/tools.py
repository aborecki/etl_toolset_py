import pandas as pd
import numpy as np
from pandas import ExcelWriter

from pyetltools import logger

def filter_pandas_dataframe_fields_by_regex(self, regex):
    return self[self.apply(lambda row: row.astype(str).str.contains(regex).any(), axis=1)]


def filter_pandas_dataframe_field_by_regex(self, column_name, regex):
    return self[self[column_name].str.contains(regex)]


pd.DataFrame.filter_by_regex = filter_pandas_dataframe_fields_by_regex
pd.DataFrame.filter_column_by_regex = filter_pandas_dataframe_field_by_regex

def save_dataframes_to_excel(dfs, sheet_names, output_file):
    logger.info("Saving dataframes to excel: "+output_file)
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


def compare_multiple_data_frames(dfs, keys, names, cols_to_compare):
    if len(dfs) != len(names):
        raise Exception("Number of dataframes should be equal to number of names.")


    # first build list of all keys

    result_df = pd.concat([ df[keys] for df in dfs ]).drop_duplicates()
    result_df["row_source_"] = ""

    for i in range(0, len(dfs)):
        result_df = compare_data_frames(result_df, dfs[i],
                                                        keys, "",
                                                        names[i], cols_to_compare)

        result_df["row_source_"] = np.where(result_df.row_source == "left", result_df.row_source_,
                                            result_df.row_source_+
                                            (" ," if i!=0 else "")  + names[i])

        result_df=result_df.drop(columns=["row_source"])
    result_df.rename(columns={'row_source_': 'row_source'}, inplace=True)
    return result_df

def compare_data_frames(df_left, df_right, keys, left_name, right_name, cols_to_compare):
    import pandas as pd


    left_name= ("_" if left_name !="" else "")+left_name
    right_name=("_" if right_name !="" else "")+right_name


    df_left = df_left.rename(columns=dict([(c, c+left_name ) for c in list(df_left.columns) if c not in keys]), inplace=False)
    df_right = df_right.rename(columns=dict([(c,c+right_name ) for c in list(df_right.columns) if c not in keys]), inplace=False)

    compare = pd.merge(df_left, df_right, on=keys, suffixes=[left_name, right_name], indicator="row_source", how="outer")

    columns = list(df_left.columns)

    def sort_columns(c):
        cr = c.replace(left_name, "").replace(right_name, "")

        if cr in cols_to_compare:
            ret= cols_to_compare.index(cr)+ (0 if left_name in c else 1)
        else:
            if cr not in columns:
                ret = 0
            else:
                ret= (columns.index(cr)+1) * 100 + (0 if left_name in c else 1)

        logger.debug("Column sorted: "+c+" -> "+str(ret))
        return ret

    cols = ["row_source"] + keys + sorted([col for col in compare if col not in keys + ["row_source"]], key=sort_columns)
    logger.debug("compare_data_frames: cols:"+ str(cols))
    compare_srt = compare[cols]
    for col_comp in cols_to_compare:
        compare_srt[col_comp+"_diff"] = ~ (
                compare_srt[col_comp + left_name] == compare_srt[col_comp + right_name])
    return compare_srt
