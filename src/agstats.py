"""
    Module
"""
import argparse
import logging
import pandas as pd
import io
import json
import requests
from enum import StrEnum
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

class NassQuickStatsUtil:
    @staticmethod
    def retrieve(args):
        if args.operation == 'parameters':
            NassQuickStatsUtil.list_parameters()
        if args.operation == 'parameter_desc':
            NassQuickStatsUtil.describe_parameter(args=args)
        if args.operation == 'count':
            NassQuickStatsUtil.count(args)
        if args.operation == 'data':
            NassQuickStatsUtil.data(args)
    @staticmethod
    def list_parameters():
        print(
            [gpt.value for gpt in NassQuickStatsParameters]
        )

    @staticmethod
    def describe_parameter(args):
        print("describe parameter:", args.parameter)
        urlstr="https://quickstats.nass.usda.gov/api/get_param_values/"
        #urlstr = (urlstr + "key=" + args.api_key
        #          + "&param=" +
        #          args.parameter)
        params = list()
        params.append(("key",args.api_key))
        params.append(("param",args.parameter))
        for qstr in args.search_conditions:
            qstrs = qstr.split(';')
            if len(qstrs) == 3:
                params.append((f'{qstrs[0]}{qstrs[1]}',qstrs[2]))
        the_response = requests.get(url=urlstr,params=params)
        print("response:", the_response.json())
        if args.parameter == "commodity_desc":
            the_c = the_response.json()["commodity_desc"]
            the_results = ";".join(the_c)
            print("the_results=", f"\"{the_results}\"",)


    @staticmethod
    def count(args):
        print("counting matches:", args.search_conditions)
        urlstr="https://quickstats.nass.usda.gov/api/get_counts/"
        params = list()
        params.append(("key",args.api_key))
        #urlstr = (urlstr + "key=" + args.api_key
        #          + "&param=" +
        #          args.parameter)
        for qstr in args.search_conditions:
            qstrs = qstr.split(';')
            if len(qstrs) == 3:
                params.append((f'{qstrs[0]}{qstrs[1]}',qstrs[2]))
        #        urlstr += "&"+qstrs[0]+qstrs[1]+"="+qstrs[2]
        the_response = requests.get(url=urlstr, params=params)
        print("response:", the_response.json())

    @staticmethod
    def data(args):
        print("getting data matches:", args.search_conditions)
        urlstr="https://quickstats.nass.usda.gov/api/api_GET/"
        params = list()
        params.append(("key",args.api_key))
        #urlstr = (urlstr + "key=" + args.api_key
        #          + "&param=" +
        #          args.parameter)
        for qstr in args.search_conditions:
            qstrs = qstr.split(';')
            if len(qstrs) == 3:
                params.append((f'{qstrs[0]}{qstrs[1]}',qstrs[2]))
        #        urlstr += "&"+qstrs[0]+qstrs[1]+"="+qstrs[2]
        the_response = requests.get(url=urlstr, params=params)
        data = the_response.json()
        df = pd.json_normalize(data["data"])
        if args.output_columns:
            o_columns = args.output_columns.split(";")
            df = df[o_columns]
        if args.output_column_names:
            o_column_names = args.output_column_names.split(";")
            df.columns = o_column_names
        #print("df=", df.columns.tolist())
        df.to_parquet(args.output)
        #print("response:", the_response.json())


class NassQuickStatsOperationType(StrEnum):
    data="data"
    data_count="count"
    parameters="parameters"
    parameter_desc="parameter_desc"


class NassQuickStatsParameters(StrEnum):
    """
    source_desc
    (Program)
    Source of data (CENSUS or SURVEY). Census program includes the Census of Ag as well as follow up projects. Survey program includes national, state, and county surveys.

    sector_desc
    (Sector)
    Five high level, broad categories useful to narrow down choices (Crops, Animals & Products, Economics, Demographics, and Environmental).

    group_desc
    (Group)
    Subsets within sector (e.g., under sector = Crops, the groups are Field Crops, Fruit & Tree Nuts, Horticulture, and Vegetables).

    commodity_desc
    (Commodity)
    The primary subject of interest (e.g., CORN, CATTLE, LABOR, TRACTORS, OPERATORS).

    class_desc
    Generally a physical attribute (e.g., variety, size, color, gender) of the commodity.

    prodn_practice_desc
    A method of production or action taken on the commodity (e.g., IRRIGATED, ORGANIC, ON FEED).

    util_practice_desc
    Utilizations (e.g., GRAIN, FROZEN, SLAUGHTER) or marketing channels (e.g., FRESH MARKET, PROCESSING, RETAIL).

    statisticcat_desc
    (Category)
    The aspect of a commodity being measured (e.g., AREA HARVESTED, PRICE RECEIVED, INVENTORY, SALES).

    unit_desc
    The unit associated with the statistic category (e.g., ACRES, $ / LB, HEAD, $, OPERATIONS).

    short_desc
    (Data Item)
    A concatenation of six columns: commodity_desc, class_desc, prodn_practice_desc, util_practice_desc, statisticcat_desc, and unit_desc.

    domain_desc
    (Domain)
    Generally another characteristic of operations that produce a particular commodity (e.g., ECONOMIC CLASS, AREA OPERATED, NAICS CLASSIFICATION, SALES). For chemical usage data, the domain describes the type of chemical applied to the commodity. The domain = TOTAL will have no further breakouts; i.e., the data value pertains completely to the short_desc.

    domaincat_desc (Domain Category)
    Categories or partitions within a domain (e.g., under domain = Sales, domain categories include $1,000 TO $9,999, $10,000 TO $19,999, etc).

    The "WHERE" (or Location) dimension
    agg_level_desc
    (Geographic Level)
    Aggregation level or geographic granularity of the data (e.g., State, Ag District, County, Region, Zip Code).

    state_ansi
    American National Standards Institute (ANSI) standard 2-digit state codes.

    state_fips_code

    NASS 2-digit state codes; include 99 and 98 for US TOTAL and OTHER STATES, respectively; otherwise match ANSI codes.

    state_alpha
    State abbreviation, 2-character alpha code.

    state_name
    (State)
    State full name.

    asd_code
    NASS defined county groups, unique within a state, 2-digit ag statistics district code.

    asd_desc
    (Ag District)
    Ag statistics district name.

    county_ansi
    ANSI standard 3-digit county codes.

    county_code
    NASS 3-digit county codes; includes 998 for OTHER (COMBINED) COUNTIES and Alaska county codes; otherwise match ANSI codes.

    county_name
    (County)
    County name.

    region_desc
    (Region)
    NASS defined geographic entities not readily defined by other standard geographic levels. A region can be a less than a state (Sub-State) or a group of states (Multi-State), and may be specific to a commodity.

    zip_5
    (Zip Code)
    US Postal Service 5-digit zip code.

    watershed_code
    US Geological Survey (USGS) 8-digit Hydrologic Unit Code (HUC) for watersheds.

    watershed_desc
    (Watershed)
    Name assigned to the HUC.

    congr_district_code
    US Congressional District 2-digit code.

    country_code
    US Census Bureau, Foreign Trade Division 4-digit country code, as of April, 2007.

    country_name
    Country name.

    location_desc
    Full description for the location dimension.

    The "WHEN" (or Time) dimension

    year
    (Year)

    4

    The numeric year of the data.

    freq_desc

    (Period Type)

    30

    Length of time covered (Annual, Season, Monthly, Weekly, Point in Time). Monthly often covers more than one month. Point in Time is as of a particular day.

    begin_code

    2

    If applicable, a 2-digit code corresponding to the beginning of the reference period (e.g., for freq_desc = Monthly, begin_code ranges from 01 (January) to 12 (December)).

    end_code

    2

    If applicable, a 2-digit code corresponding to the end of the reference period (e.g., the reference period of Jan thru Mar will have begin_code = 01 and end_code = 03).

    reference_period_

    desc (Period)

    40

    The specific time frame, within a freq_desc.

    week_ending

    10

    Week ending date, used when freq_desc = Weekly.

    load_time

    19

    Date and time indicating when record was inserted into Quick Stats database.

    The Data Value

    value

    24

    Published data value or suppression reason code.

    CV %
    Coefficient of variation. Available for the 2012 Census of Agriculture only. County-level CVs are generalized.

    """
    source_desc="source_desc"
    sector_desc="sector_desc"
    group_desc="group_desc"
    commodity_desc="commodity_desc"
    class_desc="class_desc"
    prodn_practice_desc="prodn_practice_desc"
    util_practice_desc="util_practice_desc"
    statisticcat_desc="statisticcat_desc"
    unit_desc="unit_desc"
    short_desc="short_desc"
    domain_desc="domain_desc"
    domaincat_desc="domaincat_desc"
    agg_level_desc="agg_level_desc"
    state_ansi="state_ansi"
    state_fips_code="state_fips_code"
    state_alpha="state_alpha"
    state_name="state_name"
    asd_code="asd_code"
    asd_desc="asd_desc"
    county_ansi="county_ansi"
    county_code="county_code"
    county_name="county_name"
    region_desc="region_desc"
    zip_5="zip_5"
    watershed_code="watershed_code"
    watershed_desc="watershed_desc"
    congr_district_code="congr_district_code"
    country_code="country_code"
    country_name="country_name"
    location_desc="location_desc"
    year="year"
    freq_desc="freq_desc"
    begin_code="begin_code"
    end_code="end_code"
    reference_period_desc="reference_period_desc"
    week_ending="week_ending"
    load_time="load_time"


class NassQuickStatsOperators(StrEnum):
    """
    __LE = <=
    __LT = <
    __GT = >
    __GE = >=
    __LIKE = like
    __NOT_LIKE = not like
    __NE = not equal
    """
    __LE = "<="
    __LT = "<"
    __GT = ">"
    __GE = ">="
    __LIKE = "like"
    __NOT_LIKE = "not like"
    __NE = "not equal"

#--------main-----------
def get_args():
    parser = argparse.ArgumentParser(
        description="Get Ag Stats Tool"
    )
    subparsers = parser.add_subparsers(dest="source")
    #   NASS QuickStats
    cmd_nass = subparsers.add_parser(name="nass_quickstats")
    cmd_nass.add_argument("--output",
                          dest="output",
                          type=str)
    cmd_nass.add_argument("--api-key",
                          dest="api_key",
                          type=str,
                          help="NASS QuickStats API key. Get it at https://quickstats.nass.usda.gov/api")
    cmd_nass.add_argument("--operation",
                        dest="operation",
                        type=NassQuickStatsOperationType,
                        default=NassQuickStatsOperationType.data,
                        choices=list(NassQuickStatsOperationType),
                        metavar=[gpt.value for gpt in NassQuickStatsOperationType])
    cmd_nass.add_argument("--parameter",
                          dest="parameter",
                          type=str)
    cmd_nass.add_argument("--output-columns",
                          dest="output_columns",
                          help="List of selected fields separated by semicolon",
                          type=str)
    cmd_nass.add_argument("--output-column-names",
                          dest="output_column_names",
                          help="List of corresponding field names separated by semicolon",
                          type=str)
    cmd_nass.add_argument("--search-conditions",
                          dest="search_conditions",
                          type=str,
                          nargs="*")
    args = parser.parse_args()
    return args

def agstats_main()->bool:
    args = get_args()
    print("args=", args)
    if args.source == 'nass_quickstats':
        NassQuickStatsUtil.retrieve(args)
    return True

if __name__ == "__main__":
    if not agstats_main():
        print("Failed.")
    else:
        print("Success!")