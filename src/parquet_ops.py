"""
    Module
"""
import argparse
import logging
import pandas as pd
import io
import json
import re
import os
import glob
from enum import StrEnum
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

class ParquetUtil:
    @staticmethod
    def join(args):
        #print("args=", args)
        output = args.output
        input_files = args.input_files
        input_files = ParquetUtil.expand_files(input_files=input_files)
        join_method = args.join_method
        join_field = args.join_field
        new_column = args.new_column
        new_column_regex = args.new_column_regex
        if not new_column_regex:
            new_column_regex=r"^([a-zA-Z][a-zA-Z])_.+"

        the_first_file = input_files[0]
        df1 = pd.read_parquet(the_first_file)
        if new_column:
            df1 = ParquetUtil.add_state_code_to_file(
                ag_file=the_first_file,
                df=df1,
                new_column=new_column,
                sc_reg_pattern=new_column_regex)

        for ifile in input_files[1:]:
            print(ifile)
            df2 = pd.read_parquet(ifile)
            if new_column:
                df2 = ParquetUtil.add_state_code_to_file(
                    ag_file=ifile,
                    df=df2,
                    new_column=new_column,
                    sc_reg_pattern=new_column_regex)
            df1 = pd.merge(
                df1,
                df2,
                on=join_field,
                how=join_method.value
            )
        
        df1.to_parquet(output)

    @staticmethod
    def add_state_code_to_file(
        ag_file:str, df:pd.DataFrame, new_column:str,
        sc_reg_pattern:str=r"^([a-zA-Z][a-zA-Z])_.+"):
        ret_df = df
        if not new_column:
            return ret_df
        if new_column in df.columns.tolist():
            return ret_df
        rp = re.compile(sc_reg_pattern)
        base_name = os.path.basename(ag_file)
        m = rp.match(base_name)
        if m.groups():
            new_col_default_value = m.group(1)
            ret_df[new_column] = new_col_default_value
        return ret_df


    @staticmethod
    def expand_files(input_files:list[str])->list[str]:
        ret_files = []
        for infile in input_files:
            files = glob.glob(infile)
            ret_files = ret_files + files
        return ret_files

    @staticmethod
    def get_aggregate_fields(
        data:pd.DataFrame,
        exclude_fields:list[str],
        agg_fields:str,
        agg_method:str) -> tuple[dict, list]:
        ret_fields_dict = dict()
        ret_fields=list()
        ret_field_names=list()
        if agg_fields:
            the_agg_fields = agg_fields.split(",")
            ret_fields=the_agg_fields
        else:
            column_names = data.columns.tolist()
            for coln in column_names:
                if coln not in exclude_fields:
                    ret_fields.append(coln)
        for the_f in ret_fields:
            ret_field_names.append(f'{agg_method}_{the_f}')
            ret_fields_dict[the_f] = [agg_method]
        return ret_fields_dict, ret_field_names
    
    @staticmethod
    def aggregate(args):
        output = args.output
        input = args.input
        time_fld = args.time_string_field
        time_agg_level = args.time_agg_level
        agg_method = args.agg_method
        agg_fields = args.agg_fields

        df = pd.read_parquet(input)
        df["dt_field"]=pd.to_datetime(df[time_fld])

        # Extract year, month, or day
        if time_agg_level == ParquetTemporalAggLevels.year:
            df[time_agg_level.value] = df['dt_field'].dt.year
        if time_agg_level == ParquetTemporalAggLevels.month:
            df[time_agg_level.value] = df['dt_field'].dt.month
        if time_agg_level == ParquetTemporalAggLevels.day:
            df[time_agg_level.value] = df['dt_field'].dt.day

        ret_fields, ret_fields_names=ParquetUtil.get_aggregate_fields(
            data=df,
            exclude_fields=[time_agg_level.value,
                            "dt_field",time_fld],
            agg_fields=agg_fields,
            agg_method=agg_method.value)
        #print("ret_fields=", ret_fields)
        #print("ret_fields_names=", ret_fields_names)
        gdf = df.groupby([time_agg_level.value]).agg(ret_fields)
        gdf.columns = ret_fields_names
        gdf.reset_index()
        gdf.to_parquet(output)

class ParquetJoinTypes(StrEnum):
    """
        Join method: using Panda's
    """
    inner="inner"
    left="left"
    right="right"
    outer="outer"

class ParquetTemporalAggLevels(StrEnum):
    """
        Join method: using Panda's
    """
    year="year"
    month="month"
    day="day"

class ParquetAggregateTypes(StrEnum):
    """
    count	Number of non-null observations
    sum	Sum of values
    mean	Mean of values
    mad	Mean absolute deviation
    median	Arithmetic median of values
    min	Minimum
    max	Maximum
    mode	Mode
    abs	Absolute Value
    prod	Product of values
    std	Unbiased standard deviation
    var	Unbiased variance
    sem	Unbiased standard error of the mean
    skew	Unbiased skewness (3rd moment)
    kurt	Unbiased kurtosis (4th moment)
    quantile	Sample quantile (value at %)
    cumsum	Cumulative sum
    cumprod	Cumulative product
    cummax	Cumulative maximum
    cummin	Cumulative minimum
    """
    count="count"
    sum="sum"
    mean="mean"
    mad="mad"
    median="median"
    min="min"
    max="max"
    mode="mode"
    abs="abs"
    prod="prod"
    std="std"
    var="var"
    sem="sem"
    skew="skew"
    kurt="kurt"
    quantile="quantile"
    cumsum="cumsum"
    cumprod="cumprod"
    cummax="cummax"
    cummin="cummin"

#--------main-----------
def get_args():
    parser = argparse.ArgumentParser(
        description="Parquet Tool"
    )
    subparsers = parser.add_subparsers(dest="command")
    #   JOIN
    cmd_join = subparsers.add_parser(name="join")
    cmd_join.add_argument("--output",
                          dest="output",
                          type=str)
    cmd_join.add_argument("--join-field",
                          dest="join_field",
                          type=str)
    cmd_join.add_argument("--join-method",
                        dest="join_method",
                        type=ParquetJoinTypes,
                        default=ParquetJoinTypes.inner,
                        choices=list(ParquetJoinTypes),
                        metavar=[gpt.value for gpt in ParquetJoinTypes])
    cmd_join.add_argument("--new-column",
                          dest="new_column",
                          type=str,
                          help=("New column to be added. If exists, no change will be made."))
    cmd_join.add_argument("--new-column-value-extractor",
                          dest="new_column_regex",
                          type=str,
                          help=("Regex for extracting the default value of the new column. "
                                "First group match value as the defualt"))
    cmd_join.add_argument(dest="input_files",
                          nargs='+',
                          type=str)
    # AGGREGATE
    cmd_agg = subparsers.add_parser(name="aggregate")
    cmd_agg.add_argument("--output",
                          dest="output",
                          type=str)
    cmd_agg.add_argument("--input",
                          dest="input",
                          type=str)
    cmd_agg.add_argument("--time-string-field",
                          dest="time_string_field",
                          type=str)
    cmd_agg.add_argument("--time-aggregate-level",
                          dest="time_agg_level",
                          type=ParquetTemporalAggLevels,
                          default=ParquetTemporalAggLevels.year,
                          choices=list(ParquetTemporalAggLevels),
                          metavar=[gpt.value for gpt in ParquetTemporalAggLevels])
    cmd_agg.add_argument("--aggregate-method",
                        dest="agg_method",
                        type=ParquetAggregateTypes,
                        default=ParquetAggregateTypes.mean,
                        choices=list(ParquetAggregateTypes),
                        metavar=[gpt.value for gpt in ParquetAggregateTypes])
    cmd_agg.add_argument("--aggregate-fields",
                          dest="agg_fields",
                          type=str,
                          help="A string contains column names separated by comma to be included in the aggregation. The default is try aggregate on all fields in the dataset.")
    args = parser.parse_args()
    return args

def parquet_main()->bool:
    args = get_args()
    if args.command == 'join':
        ParquetUtil.join(args)
    if args.command == 'aggregate':
        ParquetUtil.aggregate(args)
    return True

if __name__ == "__main__":
    if not parquet_main():
        print("Failed.")
    else:
        print("Success!")