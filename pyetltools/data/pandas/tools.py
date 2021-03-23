import os

import pandas as pd
import numpy as np
from pandas import ExcelWriter

from pyetltools import logger
from pyetltools.tools.misc import get_now_timestamp


def filter_pandas_dataframe_fields_by_regex(self, regex):
    return self[self.apply(lambda row: row.astype(str).str.contains(regex).any(), axis=1)]


def filter_pandas_dataframe_field_by_regex(self, column_name, regex):
    return self[self[column_name].str.contains(regex, na=False)]

def upper_all_columns(self):
    return self.apply(lambda x: x.astype(str).str.upper())

def upper_column_in_place(self, new_column_name, old_column_name=None):
    if old_column_name is None:
        old_column_name=new_column_name
    self[new_column_name]=self[old_column_name].str.upper()

def show_pandas_df_excel(self):
    ts =get_now_timestamp()
    filename = f"c:\\Temp\\output_{ts}.xlsx"
    save_dataframes_to_excel([self], ["OUTPUT"], filename)
    import os
    os.system(f"start  {filename}")

pd.DataFrame.upper_column =upper_column_in_place
pd.DataFrame.upper_all_columns =upper_all_columns
pd.DataFrame.filter_by_regex = filter_pandas_dataframe_fields_by_regex
pd.DataFrame.filter_column_by_regex = filter_pandas_dataframe_field_by_regex
pd.DataFrame.show_in_excel = show_pandas_df_excel


def save_dataframes_to_excel(dfs, sheet_names, output_file, show_in_excel=False):
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

    if show_in_excel:
        os.system(f"start  {output_file}")





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


from Levenshtein import distance as levenshtein_distance
from functools import reduce

def compare_data_frames(df_left, df_right, keys, left_name, right_name, cols_to_compare, keys_fuzzy_match=[], keys_fuzzy_match_comparers=None, make_copy=True, merge_validate=None):
    import pandas as pd

    if  keys_fuzzy_match_comparers and len(keys_fuzzy_match != keys_fuzzy_match_comparers):
        raise Exception("keys_fuzzy_match_comparers have to be None to use default fuzzy field comparer or the length of keys_fuzzy_match.")

    def default_fuzzy_comp(field_1,field_2):
        field_1_last_part = field_1.split("_")[-1]
        field_2_last_part=field_2.split("_")[-1]
        # equal when 1 char diff  and also last character is euqal
        return levenshtein_distance(field_1, field_2) <= 1 \
                and field_1_last_part == field_2_last_part

    if not keys_fuzzy_match_comparers:
        keys_fuzzy_match_comparers=[default_fuzzy_comp for x in keys_fuzzy_match]

    if make_copy:
        df_left=df_left.copy()
        df_right=df_right.copy()
    l_name = left_name
    r_name = right_name
    left_name = ("_" if left_name != "" else "") + left_name
    right_name = ("_" if right_name != "" else "") + right_name

    columns = list(df_left.columns)

    keys_to_merge = [k for k in keys if k not in keys_fuzzy_match]
    df_left['idx'] = df_left.reset_index().index
    df_right['idx'] = df_right.reset_index().index
    df_left = df_left.rename(
        columns=dict([(c, c + left_name) for c in list(df_left.columns) if c not in keys_to_merge]), inplace=False)
    df_right = df_right.rename(
        columns=dict([(c, c + right_name) for c in list(df_right.columns) if c not in keys_to_merge]), inplace=False)

    compared_df = pd.merge(df_left, df_right, on=keys_to_merge, suffixes=[left_name, right_name],
                           indicator="row_source", how="inner" if len(keys_fuzzy_match) > 0 else "outer", validate=merge_validate)
    keys_fuzzy_match_doubled=[]
    if len(keys_fuzzy_match) > 0:
        keys_fuzzy_match_doubled = reduce(lambda x, y: x + y,
                                          [[kfm + left_name, kfm + right_name] for kfm in keys_fuzzy_match])
        def filter_rows(r):
            for idx, kfm in enumerate(keys_fuzzy_match):
                field_1=r[kfm + left_name]
                field_2= r[kfm + right_name]

                ret_comp=keys_fuzzy_match_comparers[idx](field_1, field_2)
                if not ret_comp:
                    return False
            return True

        filtering_array=compared_df.apply(filter_rows, axis=1)
        if len(filtering_array)>0:
            compared_df = compared_df[filtering_array]



        # add left , right joing
        # that is  rows which are not matached
        df_left["row_source"] = "left_only"
        df_right["row_source"] = "right_only"
        compared_df = pd.concat(
            [compared_df, df_left[~ df_left["idx" + left_name].isin(compared_df["idx" + left_name])]])
        compared_df = pd.concat(
            [compared_df, df_right[~ df_right["idx" + right_name].isin(compared_df["idx" + right_name])]])

    compared_df['row_source'] = compared_df.row_source.astype(str)
    compared_df.loc[compared_df["row_source"] == "left_only", "row_source"] = l_name + "_only"
    compared_df.loc[compared_df["row_source"] == "right_only", "row_source"] = r_name + "_only"


    def sort_columns(c):
        cr = c.replace(left_name, "").replace(right_name, "")

        if cr in cols_to_compare:
            ret = (cols_to_compare.index(cr) + 1) * 10 + (0 if left_name in c else 1)
        else:
            if cr not in columns:
                ret = 0
            else:
                ret = (columns.index(cr) + 1) * 100 + (0 if left_name in c else 1)

        logger.debug("Column sorted: " + c + " -> " + str(ret))

        return ret

    cols = ["row_source"] + keys_to_merge + keys_fuzzy_match_doubled +\
           sorted([col for col in compared_df if col not in list(set(keys_to_merge).union(keys_fuzzy_match_doubled))+ ["row_source"]], key=sort_columns)
    logger.debug("compare_data_frames: cols:" + str(cols))
    compare_sorted_df = compared_df[cols]
    with pd.option_context('mode.chained_assignment', None):
        for col_comp in cols_to_compare:
            compare_sorted_df[col_comp + "_diff"] = ~ (
                    compare_sorted_df[col_comp + left_name] == compare_sorted_df[col_comp + right_name])
        compare_sorted_df["diff"]=compare_sorted_df[[ cl+"_diff" for cl in cols_to_compare ]].any(axis=1)
        compare_sorted_df["diff_cnt"] = compare_sorted_df[[cl + "_diff" for cl in cols_to_compare]].sum(axis=1)
    compare_sorted_df = compare_sorted_df.drop(columns=["idx"+left_name,"idx"+right_name])
    return compare_sorted_df

