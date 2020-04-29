"""
Script to automatically update and load coranavirus data into MySQL database.

Author: Enrique A. Pérez Delgado
Date: 28/04/2020
"""

from functools import reduce
from sqlalchemy import create_engine
import pandas as pd
import pymysql

COVID_LINKS = {
    'confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/\
                  master/csse_covid_19_data/csse_covid_19_time_series/\
                  time_series_covid19_confirmed_global.csv',
    'deaths': 'https://raw.github.com/CSSEGISandData/COVID-19/master/\
               csse_covid_19_data/csse_covid_19_time_series/\
               time_series_covid19_deaths_global.csv',
    'recovered': 'https://raw.github.com/CSSEGISandData/COVID-19/master/\
                  csse_covid_19_data/csse_covid_19_time_series/\
                  time_series_covid19_recovered_global.csv'
}


def extract_data(data_source: dict) -> dict:
    """Extract and transform data from GitHub selected links.

    Arguments:
        data_source {dict} -- Selected links for data extraction.

    Returns:
        dict -- Processed data frames for data analysis.

    """
    df_dict = {}
    for keys in data_source:
        df_dict[keys] = pd.read_csv(data_source[keys])
        df_dict[keys] = pd.melt(
            df_dict[keys],
            id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
            value_vars=list(df_dict[keys].columns[4:]),
            var_name='date',
            value_name=f'{keys}'
        )
    return df_dict


def merge_data(tables: dict):
    """Merge all data frames into a single one.

    Arguments:
        tables {dict} -- Data frames to merge.

    Returns:
        pandas.core.frame.DataFrame -- Merged data frame ready to export.

    """
    return reduce(
        lambda left, right:
        pd.merge(
            left,
            right,
            on=['Province/State', 'Country/Region', 'Lat', 'Long', 'date'],
            how='outer'),
        list(tables.values())).fillna(0)


final_data = merge_data(extract_data(COVID_LINKS))

# Connect to the database
connection = pymysql.connect(
    host='localhost',
    user='root',
    db='covid_19',
    passwd='Gearsecond900**',
    port=3306,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# Create a SQL engine
db_data = 'mysql+pymysql://root:Gearsecond900**@localhost:3306/covid_19'
sql_engine = create_engine(db_data)

# Create or replace a database table with complete data
final_data.to_sql(
    'time_series_data',
    con=sql_engine,
    if_exists='replace',
    index=False
)

sql_engine.dispose()
connection.close()
