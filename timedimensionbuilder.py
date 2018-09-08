# -*- coding: utf-8 -*-
"""
@author: James Severance
"""
import sys
import argparse
import csv
import datetime

class TimeDimensionBuilder(object):
    """
    Build a TimeDimension CSV file for use in a data warehouse/data mart.
    """
    def __init__(self):
        start = datetime.datetime(2018, 1, 1, 0, 0, 0)
        self._times = [start + datetime.timedelta(seconds=x) for x in range(0, 86400)]

    def write_to(self, file):      
        """
        Write all time records to the csv file specified in the argument.
        """
        writer = csv.writer(file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(TimeRecord.columns())
        for time_record in self.time_records():
            writer.writerow(time_record.to_list())        

    def time_records(self):
        """
        Return an iterator of TimeRecords
        """
        for time in self._times:
            yield TimeRecord(time)

class TimeRecord(object):
    """
    Construct a record of a time dimension table representing a unique second.
    """
    def __init__(self, time):
        self.time_key = int(datetime.datetime.strftime(time, '%H%M%S'))
        self.military_hour = int(datetime.datetime.strftime(time, '%H'))
        self.civilian_hour = int(datetime.datetime.strftime(time, '%I'))
        self.minute = time.minute
        self.second = time.second
        self.am_pm = datetime.datetime.strftime(time, '%p')
        self.military_time = datetime.datetime.strftime(time, '%H:%M:%S')
        self.civilian_time = datetime.datetime.strftime(time, '%I:%M:%S %p')
        self.time_class = self.time_class_for(time.hour)

    def time_class_for(self, hour):
        """
        Answer the standard time window, e.g. morning, noon, afternoon,
        evening, or night for the given hour
        """
        if hour >= 0 and hour < 6:
            result = "Night"
        elif hour >= 6 and hour < 12:
            result = "Morning"
        elif hour >= 12 and hour < 13:
            result = "Noon"
        elif hour >= 13 and hour < 17:
            result = "Afternoon"
        elif hour >= 17 and hour < 20:
            result = "Evening"
        elif hour >= 20 and hour <= 24:
            result = "Night"

        return result

    def to_list(self):
        """
        Return the TimeRecord as a list.
        """
        return [self.time_key,
                self.military_hour,
                self.civilian_hour,
                self.minute,
                self.second,
                self.am_pm,
                self.military_time,
                self.civilian_time,
                self.time_class]

    @staticmethod
    def columns():
        """
        Return the column headings for all TimeRecords
        """
        return ['time_key', 'military_hour', 'civilian_hour', 'minute',
                'second', 'am_pm', 'military_time', 'civilian_time', 'time_class']

def main():
    """
    Build the time dimension sending contents either to stdout or a file if specified in stdin.
    """
    parser = argparse.ArgumentParser(
        description="Build a Time Dimension for use in a Data Warehouse or Data Mart. "
        + "The output will be written to stdout unless a CSV file "
        + "is specified with the -f/--file option.")
    parser.add_argument("-f",
                        "--file",
                        dest="filename",
                        help="write output to a CSV file",
                        metavar="FILE")
    args = parser.parse_args()

    builder = TimeDimensionBuilder()
    if args.filename is None:
        builder.write_to(sys.stdout)
    else:
        with open(args.filename, 'w', newline='') as dim_time_file:
            builder.write_to(dim_time_file)
        print("Time Dimension written to " + args.filename)

if __name__ == '__main__':
    main()
