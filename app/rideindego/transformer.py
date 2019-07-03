import os
import pandas as pd
from functools import reduce
from config import get_config as _get_config

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

        # combine all dataframe
        # beware of missing data: bike_type
        # bike_type was added in 2018 q3
        df_all = pd.concat(df_array)
        df_all['bike_type'] = df_all.bike_type.fillna('standard')

        # link pointer to class attributes
        self.trip_data = df_all

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

        self._load_all_trip_data()
        self._save_all_trip_data()

        return {
            'data_file': self.combined_data_path
        }


if __name__ == "__main__":
    cleaner_config = _CONFIG.get('etl', {}).get('trip_data',{})
    cleaner = TripDataCleansing(cleaner_config)
    cleaner.pipeline()

    print('END OF GAME')

