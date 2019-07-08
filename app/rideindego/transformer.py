import datetime
import os
from functools import reduce

import pandas as pd

from rideindego.parse_config import get_config as _get_config

_CONFIG = _get_config()


class TripDataCleansing():
    """Load, transform, and Dump trip data
    """

    def __init__(self, config):
        self.config = config
        self.__parse_config()

    def __parse_config(self):
        """Decompose the config of the class and build attribute
        """
        if not self.config.get('datapath'):
            raise Exception('Did not define datapath')
        else:
            datapath = self.config.get('datapath')
            self.datapath = f'{os.sep}' + f'{os.sep}'.join(datapath)

        try:
            self.data_files = os.listdir(self.datapath)
            # get the csv data files
            self.data_files = [
                os.path.join(self.datapath, i)
                for i in self.data_files
                if i.endswith('.csv')
            ]
        except Exception as ee:
            raise Exception(f'Could not listdir for {self.datapath}')

        # assign output data file path
        if not self.config.get('combined_data_path'):
            raise Exception('Did not define combined_data_path')
        else:
            combined_data_path = self.config.get('combined_data_path')
            self.combined_data_path = f'{os.sep}' + f'{os.sep}'.join(
                combined_data_path
                )

    def _load_all_trip_data(self):
        """Load all trip data from config data path
        """

        # Load all csv files into dataframes
        df_array = [pd.read_csv(csv_file) for csv_file in self.data_files]
        # Rename *_station to *_station_id
        # they have two different types of column names for stations
        df_array = [
            df_temp.rename( columns={'end_station': 'end_station_id', 'start_station': 'start_station_id'} )
            for df_temp in df_array
        ]

        # combine all dataframe
        # beware of missing data: bike_type
        # bike_type was added in 2018 q3
        df_all = pd.concat(df_array)

        # link pointer to class attributes
        self.trip_data = df_all

    def _datetime_transformations(self):
        """Standardize datetime formats
        """

        # extract date from datetime strings
        # they have different formats for dates so it is easier to
        # use pandas
        self.trip_data['date'] = self.trip_data.start_time.apply(
            lambda x: x.split(' ')[0] if x else None
            )
        self.trip_data['date'] = pd.to_datetime(self.trip_data.date)

        # extract hour of the day
        # there exists different time formats
        self.trip_data['hour'] = self.trip_data.start_time.apply(
            lambda x: int(float(x.split(' ')[-1].split(':')[0]))
            )

        # get weekday
        self.trip_data['weekday'] = self.trip_data.date.apply(
            lambda x: x.weekday()
            )

        # get month
        self.trip_data['month'] = self.trip_data.date.apply(
            lambda x: x.month
        )

    def _duration_normalization(self):
        """Duration was recorded as seconds before 2017-04-01.

        Here we will normalized durations to minutes
        """

        df_all_before_2017q1 = self.trip_data.loc[
            self.trip_data.date < datetime.date(2017,4,1)
            ]
        df_all_after_2017q1 = self.trip_data.loc[
            self.trip_data.date >= datetime.date(2017,4,1)
            ]

        df_all_before_2017q1['duration'] = df_all_before_2017q1.duration/60

        self.trip_data = pd.concat(
            [df_all_before_2017q1, df_all_after_2017q1]
            )

    def _fill_station_id(self):
        """start_station_id has null values

        fillna with 0 for the station id
        """
        self.trip_data['start_station_id'].fillna(0, inplace=True)

    def _backfill_bike_types(self):
        """Bike types did not exist until q3 of 2018
         because they only had standard before this.
        """

        self.trip_data['bike_type'] = self.trip_data.bike_type.fillna('standard')

    def _save_all_trip_data(self):
        """Dump all trip data to the destination define in config
        """

        try:
            self.trip_data.to_csv(self.combined_data_path, index=False)
        # TODO: should be more specific about the exceptions
        except Exception as ee:
            raise Exception(f'Could not save data to {self.combined_data_path}')


    def _load_station_info(self):
        """TODO: write the function to load station info
        """

        return

    def _load_station_status(self):
        """TODO: write the function to load station status
        """

        return

    def pipeline(self):
        """Connect the pipes of data operations
        """

        # load all csv files to one dataframe
        self._load_all_trip_data()

        # Transformations
        self._datetime_transformations()
        self._duration_normalization()
        self._backfill_bike_types()
        self._fill_station_id()

        # dave data
        self._save_all_trip_data()

        return {
            'data_file': self.combined_data_path
        }


if __name__ == "__main__":
    cleaner_config = _CONFIG.get('etl', {}).get('trip_data',{})
    cleaner = TripDataCleansing(cleaner_config)
    cleaner.pipeline()

    print('END OF GAME')
