# -*- coding: utf-8 -*-
"""
@author: James Severance
TODO: Consider incorporating other columns identified in The Data Warehouse ETL Toolkit p. 171
"""
import sys
import argparse
import datetime
import calendar
import math
import csv
import numpy
import holidays

# Set Calendar's first day of the week to Sunday
calendar.setfirstweekday(6)

class DateDimensionBuilder(object):
    """
    Builds a date dimension, AKA a calendar dimension, for use in data warehousing.
    """

    def __init__(self, start_date, end_date):
        """
        Constructs a new DateDimensionBuilder starting at start_date and ending at end_date.
        """
        self.start_date = start_date
        self.end_date = end_date

    def date_records(self):
        """
        Returns a generator to iterate over each date record of the calendar.
        """
        for ordinal in range(self.start_date.toordinal(), self.end_date.toordinal()+1):
            yield DateRecord(datetime.date.fromordinal(ordinal))

    def write_to(self, file, columnsonly=False):
        """
        Write all date records to a csv file.
        """
        writer = csv.writer(file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        writer.writerow(DateRecord.columns())
        if columnsonly is False:
            for date_record in self.date_records():
                writer.writerow(date_record.to_list())

class DateRecord(object):
    """
    Represents a date record in a date dimension.
    """

    def __init__(self, date):
        """
        Construct a new date record for the given date.
        """
        self._current_date = date

    @property
    def date_key(self):
        """
        Retruns an int representation of the date suitable
        for use as a primary key in a date dimension table
        """
        return int(self._current_date.strftime("%Y%m%d"))

    @property
    def date(self):
        """
        Returns a character representation of the date in
        YYYY-MM-DD format for importing into a date dimension
        table as a date data type
        """
        return self._current_date.strftime("%Y-%m-%d")

    ###
    # Seasons
    @property
    def season(self):
        """
        Returns the season, i.e. Spring, Summer, Fall, or Winter.
        """
        if self.day_of_year in range(80, 172):
            season = 'Spring'
        elif self.day_of_year in range(172, 264):
            season = 'Summer'
        elif self.day_of_year in range(264, 355):
            season = 'Fall'
        else:
            season = 'Winter'

        return season

    ###
    # Quarters
    @property
    def quarter(self):
        """
        Returns the current quarter as an int, 1, 2, 3, or 4.
        """
        return math.ceil(self._current_date.month/3)

    @property
    def quarter_name(self):
        """
        Returns the quarter name, e.g. First, Second, Third, or Fourth
        """
        quarters = {1: "First", 2: "Second", 3: "Third", 4: "Fourth"}
        return quarters[self.quarter]

    @property
    def quarter_short_name(self):
        """
        Returns the short name of the quarter, i.e. Q1, Q2, Q3, or Q4
        """
        quarters = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
        return quarters[self.quarter]

    ###
    # Year Methods
    @property
    def year(self):
        """
        Returns the year as an int.
        """
        return int(self._current_date.strftime("%Y"))

    @property
    def fiscal_year(self):
        """
        Returns the fiscal year if different from the calendary year. If
        not different, it returns the calendar year.
        """
        #TODO: Implement this.
        return None

    @property
    def year_and_quarter(self):
        """
        Returns the YYYY/Qn
        """
        return str(self.year) + "/" + self.quarter_short_name

    @property
    def year_and_month(self):
        """
        Returns the year and month as YYYY/MM.
        """
        return self._current_date.strftime("%Y/%m")

    @property
    def year_and_month_abbrev(self):
        """
        Returns the year and month short name, e.g. YYYY/Oct.
        """
        return str(self.year) + "/" + self.month_abbrev

    ###
    # Month Methods
    @property
    def month_number(self):
        """
        Returns an int representing the calendar month.
        """
        return int(self._current_date.strftime("%m"))

    @property
    def month_name(self):
        """
        Returns the long month name, e.g. January, December.
        """
        return self._current_date.strftime("%B")

    @property
    def month_abbrev(self):
        """
        Returns an abbreviated month name, e.g. Jan, Dec.
        """
        return self._current_date.strftime("%b")

    @property
    def month_end_flag(self):
        """
        Answers true if the day is the last day of the month.
        """
        mrange = calendar.monthrange(self.year, self.month_number)
        if mrange[1] == self.day_of_month:
            return True
        else:
            return False

    @property
    def fiscal_month_number(self):
        """
        Answers the month number for the fiscal calendar. If no fiscal start
        month is set, this answers with month_number().
        """
        #TODO: Implement this.
        return None

    ###
    # Week Methods
    @property
    def week_num_in_year(self):
        """
        Return the week's number in the calendar year.
        """
        week = int(self._current_date.strftime("%U"))
        if week < 1:
            week = 1
        return week

    @property
    def week_num_in_month(self):
        """
        Returns the week number in the month.
        """
        mcalendar = calendar.monthcalendar(self._current_date.year, self._current_date.month)
        matrix = numpy.array(mcalendar)
        match = numpy.where(matrix == self.day_of_month)
        week_of_month = match[0][0] + 1
        return week_of_month

    @property
    def week_begin_date_key(self):
        """
        Returns the Sunday of the week for the current date as an int for use as a key
        """
        week_begin_date = datetime.datetime.strptime(self.week_begin_date, "%Y-%m-%d")
        return week_begin_date.strftime("%Y%m%d")

    @property
    def week_begin_date(self):
        """
        Returns the Sunday of the week for the current date as a YYYY-MM-DD formatted string
        """
        if self.day_name == 'Sunday':
            return self.date
        else:
            return (self._current_date
                    - datetime.timedelta(days=self.day_of_week)).strftime("%Y-%m-%d")

    ###
    # Day Methods
    @property
    def day_of_year(self):
        """
        Returns the day of the year, 1..365.
        """
        return self._current_date.timetuple().tm_yday

    @property
    def day_of_month(self):
        """
        Returns an int representing the day of the month.
        """
        return int(self._current_date.strftime("%d"))

    @property
    def day_of_week(self):
        """
        Returns the day's number, with 1 being Sunday and 7 being Saturday.
        """
        return ((self._current_date.weekday() + 1) % 7) + 1

    @property
    def day_name(self):
        """
        Answers the day's name in long form, e.g. Monday
        """
        return self._current_date.strftime("%A")

    @property
    def day_name_abbrev(self):
        """
        Answers the day's name abbreveiated, e.g. Mon
        """
        return self._current_date.strftime("%a")

    @property
    def same_day_previous_year_key(self):
        """
        Returns an int representation of this date one year ago, e.g. YYYYMMDD.
        If the present day is a leap day, the prior day of the previous year
        will be returned.
        """
        return datetime.datetime.strptime(self.same_day_previous_year,
                                          "%Y-%m-%d").strftime("%Y%m%d")

    @property
    def same_day_previous_year(self):
        """
        YYYY-MM-DD formatted version of same_day_previous_year_key
        """
        year = self._current_date.year -1
        month = self._current_date.month
        day = self._current_date.day
        try:
            one_year_ago = datetime.date(year, month, day)
        except ValueError:
            # Error due to leap year. Use the date for the
            # previous year less a day.
            one_year_ago = datetime.date(year, month, day-1)
        return one_year_ago.strftime("%Y-%m-%d")

    @property
    def is_weekday(self):
        """
        Answers True if not Saturday or Sunday.
        """
        if self.day_name_abbrev == 'Sat' or self.day_name_abbrev == 'Sun':
            return False
        else:
            return True

    @property
    def is_weekend(self):
        """
        Answers True if Saturday or Sunday.
        """
        return not self.is_weekday

    ###
    # Holidays
    @property
    def is_holiday(self):
        """
        True if date is a standard US holiday.
        """
        us_holidays = holidays.UnitedStates()
        return self._current_date in us_holidays

    @property
    def holiday_name(self):
        """
        Holiday name if there is one, None if not.
        """
        us_holidays = holidays.UnitedStates()
        return us_holidays.get(self._current_date.strftime("%Y-%m-%d"))

    def to_list(self):
        """
        Returns a list representation of the record.
        """
        return [getattr(self, value) for value in dir(self)
                if value not in ("to_list", "columns") and value[0] is not "_"]

    @staticmethod
    def columns():
        """
        Returns the column names corresponding to to_list().
        """
        return [value for value in dir(DateRecord)
                if value not in ("to_list", "columns") and value[0] is not "_"]

def main():
    """
    Build the date dimension sending contents either to stdout or a file if specified in stdin.
    """
    parser = argparse.ArgumentParser(
        description="Build a Date Dimension for use in a Data Warehouse or Data Mart. "
        + "The output will be written to stdout unless a CSV file "
        + "is specified with the -f/--file option.")
    parser.add_argument("-f",
                        "--file",
                        dest="filename",
                        help="write output to a CSV file.",
                        metavar="FILE")
    parser.add_argument("-s",
                        "--startdate",
                        dest="startdate",
                        #type="string",
                        default="1/1/1850",
                        help="starting date of the date dimension. Default is 1/1/1850.",
                        metavar="DATE")
    parser.add_argument("-e",
                        "--enddate",
                        dest="enddate",
                        #type="string",
                        default="12/31/2050",
                        help="ending date for the date date dimesnion. Default is 12/31/2050.",
                        metavar="DATE")
    parser.add_argument("-c",
                        "--columnnamesonly",
                        dest="columnsonly",
                        action='store_true',
                        help="output column names only.")
    args = parser.parse_args()

    try:
        start_date = datetime.datetime.strptime(args.startdate, "%m/%d/%Y")
        end_date = datetime.datetime.strptime(args.enddate, "%m/%d/%Y")
        builder = DateDimensionBuilder(start_date, end_date)
        if args.columnsonly is True:
            builder.write_to(sys.stdout, columnsonly=True)
        elif args.filename is None:
            builder.write_to(sys.stdout)
        else:
            with open(args.filename, 'w', newline='') as dim_date_file:
                builder.write_to(dim_date_file)
            print("Date Dimension written to " + args.filename)
    except ValueError as err:
        print("Unable to parse supplied dates "
              + args.startdate
              + ", and "
              + args.enddate
              + ". Error detail: "
              + str(err))
        parser.print_help()

if __name__ == '__main__':
    main()
