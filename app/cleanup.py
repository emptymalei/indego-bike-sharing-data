import pandas as pd
from functools import reduce
import os
import datetime
from loguru import logger
import click
import numpy as np

def get_files(wd):

    data_files = os.listdir(wd)

    csv_files = [os.path.join(wd,i) for i in data_files if i.endswith('.csv')]

    logger.info(f"The files are\n{csv_files}")

    return csv_files

def cleanup_for_parquet(dataframe):

    dataframe["start_lat"] = dataframe.start_lat.apply(lambda x: float(x) if x != "\\N" else np.nan)
    dataframe["start_lon"] = dataframe.start_lon.apply(lambda x: float(x) if x != "\\N" else np.nan)
    dataframe["end_lon"] = dataframe.end_lon.apply(lambda x: float(x) if x != "\\N" else np.nan)

    dataframe["end_lat"] = dataframe.end_lat.apply(lambda x: float(x) if x != "\\N" else np.nan)

    dataframe["bike_id"] = dataframe.bike_id.apply(lambda x: str(x))

    pass



@click.command()
@click.option(
    "-w", "--wd", type=click.Path(exists=True),
    help="Path to working directory where the files live"
)
def main(wd=None):

    if wd is None:
        wd = '/tmp/rideindego'

    csv_files = get_files(wd)

    logger.info("Combining all files ...")
    df_all = pd.DataFrame()

    df_array = [pd.read_csv(csv_file) for csv_file in csv_files]

    for i in df_array:
        logger.info(
            'columns:', len(i.columns), '; index:', i.index, '; length:', len(i)
        )

    df_array = [
        df_temp.rename( columns={'end_station': 'end_station_id', 'start_station': 'start_station_id'} )
        for df_temp in df_array
    ]

    logger.info("Concat all files into one dataset...")
    df_all = pd.concat(df_array)

    logger.info(
        'all unique bike_ids:', df_all.bike_id.nunique(),
        'unique bike_ids with bike_type:', df_all[~df_all.bike_type.isna()].bike_id.nunique()
    )

    logger.info("Checking the different bike types")
    df_bike_type = df_all[['bike_id', 'bike_type']].drop_duplicates()
    logger.info(
        df_bike_type.describe()
    )

    logger.info("Filling in standard as default bike_type...")
    df_all['bike_type'] = df_all.bike_type.fillna('standard')

    logger.info("Dealing with datetime...")
    df_all['date'] = df_all.start_time.apply(lambda x: x.split(' ')[0] if x else None)
    df_all['date'] = pd.to_datetime(df_all.date)
    df_all['hour'] = df_all.start_time.apply(lambda x: int(float(x.split(' ')[-1].split(':')[0])) )

    # The units for duration changed on 2017-04-01
    # It is faster to split the dataframe than using apply
    df_all_before_2017q1 = df_all.loc[df_all.date < pd.to_datetime("2017-04-01")]
    df_all_after_2017q1 = df_all.loc[df_all.date >= pd.to_datetime("2017-04-01")]
    df_all_before_2017q1['duration'] = df_all_before_2017q1.duration/60
    df_all_clean = pd.concat([df_all_before_2017q1, df_all_after_2017q1])

    # get weekday
    df_all_clean['weekday'] = df_all_clean.date.apply(lambda x: x.weekday() + 1)

    # get month
    df_all_clean['month'] = df_all_clean.date.apply(lambda x: x.month)

    cleanup_for_parquet(df_all_clean)

    df_all_clean.to_csv(
        os.path.join(
            wd,
            "combined_indego.csv"
        ),
        index=False
    )

    df_all_clean.to_parquet(
        os.path.join(
            wd,
            "combined_indego.parquet"
        ),
        index=False
    )



if __name__ == "__main__":

    main()

    pass