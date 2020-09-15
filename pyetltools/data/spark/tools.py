from functools import reduce


import pandas
import pyspark
from pyspark.sql.types import BooleanType, DateType, LongType, IntegerType, FloatType, StringType, StructField, \
    StructType
from pyspark.sql import functions as F


def df_to_excel(df, filename):
    df2 = df.select([F.col(c).cast("string") for c in df.columns])
    df2.toPandas().to_excel(filename, sheet_name='DATA', index=False)


def df_to_csv(df, output_dir):
    df.coalesce(1).write.option("header", "true").option("sep", ",").mode("overwrite").csv(output_dir)


def compare_dataframes(left_df, right_df, key_fields, exclude_columns=[], include_columns=[], mode=1, add_diff_cont_cols=False):

    from collections import namedtuple

    ComparisionReport = namedtuple("ComparisionRaport", ["counts_are_equal","left_count","right_count","diff_count", "mode", "comp_details"])

    if 1 > mode > 3:
        print("Allowed modes 1 (counts only), 2 (only counts for content), 3 (full - content)")

    left_df.persist()
    right_df.persist()

    left_cnt = left_df.count();
    right_cnt = right_df.count();


    if left_cnt != right_cnt:
        counts_are_equal=False
        print("Datasets counts are not equal.")
        print("Left count: " + str(left_cnt))
        print("Right count: " + str(right_cnt))
        print("Difference: " + str(right_cnt - left_cnt))
    else:
        counts_are_equal = True
        print("Datasets counts equal.")
        print("Left/right count:" + str(left_cnt))


    if len(key_fields) >0:
        left_key_dist_cnt = left_df.select([x for x in key_fields]).distinct().count()
        right_key_dist_cnt = right_df.select([x for x in key_fields]).distinct().count()
        stop = False;
        print()
        if left_key_dist_cnt != left_cnt:
            print("Key columns (" + str(key_fields) + ") do not distinct in left dataset")
            stop = True
        if right_key_dist_cnt != right_cnt:
            print("Key columns (" + str(key_fields) + ") do not distinct in right dataset")
            stop = True
        if stop:
            return

    if mode == 1:
        return ComparisionReport(counts_are_equal, left_cnt, right_cnt, right_cnt - left_cnt, mode, None);

    if len(key_fields) == 0:
        print("No keys defined")

    common_cols = sorted(set(left_df.columns).intersection(right_df.columns), key=lambda x: left_df.columns.index(x))
    common_cols = [x for x in common_cols if x not in exclude_columns]
    print("Common columns (without excluded):" + str(common_cols))
    print("Join cols:" + str(key_fields))
    join_cond = [left_df[c] == right_df[c] for c in key_fields]
    if mode == 2:
        select_cols = key_fields
    if mode == 3:
        select_cols = common_cols


    join_res_df = left_df.join(right_df, join_cond, how="full")

    # F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)
    join_res_df = join_res_df.withColumn("left_missing",
                                         F.when(reduce(lambda a, b: a & b, [left_df[x].isNull() for x in key_fields]),
                                                1).otherwise(0))
    join_res_df = join_res_df.withColumn("right_missing",
                                         F.when(reduce(lambda a, b: a & b, [right_df[x].isNull() for x in key_fields]),
                                                1).otherwise(0))
    join_res_df = join_res_df.withColumn("both",
                                         F.when(reduce(lambda a, b: a & b,
                                                       [left_df[x].isNotNull() & right_df[x].isNotNull() for x in
                                                        key_fields]),
                                                1).otherwise(0))
    #for c in key_fields:
    #    join_res_df=join_res_df.drop(right_df[c])

    content_cols = [x for x in select_cols if x not in key_fields]
    if len(include_columns) > 0:
        content_cols = include_columns
    print("Content cols:" + str(content_cols))

    def is_float(value):
        try:
            float(value)  # for int, long and float
        except ValueError:
            return False
        except TypeError:
            return False
        return True

    is_float_udf = F.udf(is_float, BooleanType())

    for col in content_cols:
        join_res_df = join_res_df.withColumn("diff_" + col, F.when(
            F.when(is_float_udf(left_df[col]), left_df[col].cast("float")).otherwise(left_df[col].cast("string"))
            != F.when(is_float_udf(right_df[col]), right_df[col].cast("float")).otherwise(right_df[col].cast("string")),
            1).otherwise(0))
        if add_diff_cont_cols:
            join_res_df = join_res_df.withColumn("diff_cont_" + col, F.when(
            F.when(is_float_udf(left_df[col]), left_df[col].cast("float")).otherwise(left_df[col].cast("string"))
            != F.when(is_float_udf(right_df[col]), right_df[col].cast("float")).otherwise(right_df[col].cast("string")),
            F.concat(left_df[col], F.lit("|||"), right_df[col])).otherwise(""))
    if (len(content_cols) > 0):
        join_res_df = join_res_df.withColumn("diff_cnt", reduce(lambda a, b: a + b,
                                                                [join_res_df["diff_" + x] for x in content_cols]))
        if add_diff_cont_cols:
            join_res_df = join_res_df.withColumn("diff_cont", F.when(reduce(lambda a, b: a + b,
                                                                        [join_res_df["diff_" + x] for x in
                                                                         content_cols]) == 0, 0).otherwise(1))

    join_res_df.persist();
    # join_res_df.show();

    if (len(content_cols) > 0):
        cols=["left_missing", "right_missing", "both", "diff_cnt"]
        agg_df = join_res_df.select(cols).agg(
            F.sum("left_missing").alias("left_missing"),
            F.sum("right_missing").alias("right_missing"),
            F.sum("both").alias("both"),
            F.sum("diff_cnt").alias("diffrent_rows_cnt"))
    else:
        agg_df = join_res_df.select(["left_missing", "right_missing", "both"]).agg(
            F.sum("left_missing").alias("left_missing"),
            F.sum("right_missing").alias("right_missing"),
            F.sum("both").alias("both"))

    # agg_df.show()
    left_missing_cnt = agg_df.select("left_missing").first()[0]
    right_missing_cnt = agg_df.select("right_missing").first()[0]
    both_cnt = agg_df.select("both").first()[0]
    if len(content_cols) > 0:
        diff_cnt = agg_df.select("diffrent_rows_cnt").first()[0]

    print("")
    print("Count of rows from right dataset missing in left dataset:" + str(left_missing_cnt))
    print("Count of rows from left dataset missing in right dataset:" + str(right_missing_cnt))
    print("Count of rows found in both datasets: " + str(both_cnt))
    if len(content_cols) > 0:
        print("Count of diffrent rows: " + str(diff_cnt))

    # change names in columns to remove duplicates
    join_res_df = join_res_df.select([left_df[c].alias("left_"+c) for c in dict.fromkeys([x for x in join_res_df.columns
                                                                                          if x in left_df.columns])]+
                                     [right_df[c].alias("right_"+c) for c in dict.fromkeys([x for x in join_res_df.columns if x in right_df.columns])]+
                                     [join_res_df[c] for c in
                                      [x for x in join_res_df.columns if x not in right_df.columns and x not in left_df.columns]]
                                     )

    return ComparisionReport(counts_are_equal, left_cnt, right_cnt, right_cnt - left_cnt, mode, join_res_df)



# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]':
        return DateType()
    if f == '<M8[ns]':
        return DateType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def convert_pandas_df_to_spark(spark, pandas_df):
    columns = list(pandas_df.columns)

    struct_list = []

    #infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
    # pandas_df.apply(infer_type, axis=0)

    df_types = list(pandas.DataFrame(pandas_df.apply(pandas.api.types.infer_dtype, axis=0)).reset_index().rename(
        columns={'index': 'column', 0: 'type'}));
    types = list(pandas_df.dtypes)
    # pandas_df=pandas_df.astype(df_types)
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    print(p_schema)
    try:
        return spark.createDataFrame(pandas_df).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)
    except:
        print("PD2SparkDF conv failed on default schema inference. Using schema override.")
        return spark.createDataFrame(pandas_df, p_schema).persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER)


