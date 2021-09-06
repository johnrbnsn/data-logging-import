"""
2021-09-01
John Robinson

This project aims to provide a simple interface to take data logged for automotive use from various sources
and have it converted to a uniform format. The primary data axis will be time, and secondary will be
distance. This data may be a continuous stream, or split into one or more laps along a closed course.

Minimum of Python 3.7 due to the use of dataclasses.

Currently supported input file formats are:
    * AiM
"""
from dataclasses import dataclass
import re

import pandas as pd


@dataclass
class DataLog:
    """
    Class for converting imported data to the standard format.

    >>> dl = DataLog('sample_data/YamahaR6.csv')
    >>> dl.metadata['Duration']
    '542.771'
    """
    filepath: str           # String with the path to the datafile to be converted
    dataframe: pd.DataFrame = pd.DataFrame()
    version: str = '0.1'    # Version of DataLog

    def __post_init__(self):
        """
        Attempt to convert the file at DataLog.filepath to the standard data format
        """
        self.convert_aim_file()

    def convert_aim_file(self):
        """
        Converts an AiM file to the standard format
        """
        self._read_aim_headers()
        skiprows = self.metadata['metadata_row_nums'] + self.metadata['header_row_nums']
        skiprows.remove(self.metadata['headings_row_num'])
        # skiprows.remove(metadata['units_row_num'])
        # Note: AiM exports csv files in "ISO-8859-1" format
        self.dataframe = pd.read_csv(self.filepath, skiprows=skiprows, encoding="ISO-8859-1")

        self._parse_aim_laptime_and_totaltime()

    def _parse_aim_laptime_and_totaltime(self):
        """
        AiM files report laptime only, and do not have a separate total time.
        """
        lap_start_index = self.dataframe.index[self.dataframe['Time'] == 0].tolist()
        lap_end_index = [lsi - 1 for lsi in lap_start_index[1:]] + [len(self.dataframe) - 1]

        # Split into laps
        lap_number = 0
        total_lap_time = 0
        for lap_index in lap_start_index:
            lap_number += 1
            self.dataframe.loc[lap_index:, 'Lap #'] = int(lap_number)

        self.dataframe['Total Time'] = self.dataframe['Time'].copy()
        for ii in range(len(lap_end_index)):
            lap_start_idx = lap_start_index[ii]
            lap_end_idx = lap_end_index[ii]
            self.dataframe.loc[
                lap_start_idx:lap_end_idx, 'Total Time'
            ] = total_lap_time + self.dataframe.loc[lap_start_idx:lap_end_idx:, 'Time']
            total_lap_time = self.dataframe.loc[lap_end_idx, 'Total Time']

    def _read_aim_headers(self):
        """
        Parses the headers of an AiM file
        """
        metadata = {}
        metadata_row_nums = []
        reading_metadata = True
        line_num = 0
        reading_headers = False
        header_rows = []
        header_row_nums = []
        with open(self.filepath) as f:
            ln = f.readline()
            while ln:
                if reading_metadata:
                    if ln.startswith('"'):
                        metadata_key = ln.split(',')[0].replace('"', '')
                        metadata_value = ln.split(',')[1].replace('"', '').strip('\n')
                        metadata[metadata_key] = metadata_value
                    elif ln.startswith('\n'):
                        # Line break, no longer dealing with metadata
                        reading_metadata = False
                        reading_headers = True
                    metadata_row_nums.append(line_num)
                elif reading_headers:
                    if ln.startswith('"'):
                        header_row_data = ln.strip('\n').split(',')
                        header_row = [hdr.replace('"', '') for hdr in header_row_data]

                        if re.search('^Time|Distance', ln.strip('"')):
                            metadata['headings_row_num'] = line_num
                        elif re.search('^sec|km', ln.strip('"')):
                            metadata['units_row_num'] = line_num
                        header_rows.append(header_row)
                    elif re.search('^\d+', ln):
                        # Found the start of the data, break
                        break
                    elif ln.startswith('\n'):
                        # Second line break, before data starts
                        header_row_nums.append(line_num)
                        break
                    header_row_nums.append(line_num)
                ln = f.readline()
                line_num += 1
            metadata['header_row_nums'] = header_row_nums
            metadata['metadata_row_nums'] = metadata_row_nums

            self.metadata = metadata


if __name__ == "__main__":
    import doctest
    doctest.testmod()
