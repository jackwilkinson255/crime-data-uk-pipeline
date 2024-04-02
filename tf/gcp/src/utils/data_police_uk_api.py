import wget
import logging
import time
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger('root')

class DataPoliceUKAPI():

    def get_last_updated(self):
        """Get the date the database was last updated"""
        try:
            response = requests.get('https://data.police.uk/api/crime-last-updated')
            response.raise_for_status() # raise an exception if the request was unsuccessful
        except requests.exceptions.RequestException as err:
            logging.error(f"An error occurred: {err}")
            raise Exception(f"An error occurred: {err}")
        last_updated = response.json()
        return last_updated["date"]


    def get_zip_file(self, month, temp_file_path):
        """Downloads batch compressed data from API and stores in temp directory"""
        url = f"https://data.police.uk/data/archive/{month}.zip"
        logger.info(f"Downloading file from {url}...")

        start_time = time.time()
        wget.download(url, out=temp_file_path)
        end_time = time.time()

        elapsed_time = end_time - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        logger.info(f"Download complete after {minutes}m {seconds}s!")
        logger.info(f"Saving temp file to: {temp_file_path}")


    def validate_message(self, cloud_event_data):
        """Check we have the keys required in our dictionary"""
        try:
            months = cloud_event_data["months"]
            data_sets = cloud_event_data["data_sets"]
            logger.info(f"Months to query: {months}\nCrime data to load: {data_sets}")
        except KeyError as err:
            logger.error("months or data_sets key are missing!")
            raise KeyError(f"months or data_sets key are missing!")

        return months, data_sets


    def validate_months(self, months):
        api_request_data = {}
        if months == "all":
            api_request_data["interval_months"], api_request_data["record_months"] = self.get_months_and_intervals()
        elif len(months) > 1:
            api_request_data["interval_months"] = self.get_months_and_intervals(months)
            api_request_data["record_months"] = months
        else:
            api_request_data["interval_months"] = months
            api_request_data["record_months"] = months

        return api_request_data


    def get_months_and_intervals(self, months=None):
        """
        Get all months between records start (2010-12-01) to the latest_update
        Get the API query months we need for all these months. As the API call
        for a month contains the previous three years of data.
        """
        if months:
            return self.get_range_months_intervals(months)
        else:
            latest_date = self.get_last_updated()

            records_start_month = datetime(2010, 12, 1).date()
            records_end_month = datetime.strptime(latest_date, '%Y-%m-%d').date()
            interval_start_month = datetime(2017, 4, 1).date()

            # Get month intervals for API call
            interval_months = self.get_month_intervals(interval_start_month, records_end_month)

            # Get months of data received
            records_months = self.get_records_months(records_start_month, records_end_month)

            return interval_months, records_months


    def get_range_months_intervals(self, months):
        dates = [datetime.strptime(month, "%Y-%m") for month in months]
        max_date = max(dates)
        min_date = min(dates)
        diff = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)

        max_date_year_month = max_date.strftime('%Y-%m')
        month_window = 36
        if diff < month_window:
            return [max_date_year_month]


    def get_month_intervals(self, interval_start, records_end):
        interval_months = []
        while interval_start <= records_end:
            interval_months.append(interval_start.strftime('%Y-%m'))
            interval_start = interval_start + relativedelta(years=3)
        if records_end.strftime('%Y-%m') not in interval_months:
            interval_months.append(records_end.strftime('%Y-%m'))

        return interval_months


    def get_records_months(self, records_start_month, records_end_month):
        records_months = []
        while records_start_month <= records_end_month:
            records_months.append(records_start_month.strftime('%Y-%m'))
            records_start_month = records_start_month + relativedelta(months=1)

        return records_months


    def validate_crime_data_sets(self, data_sets):
        valid_crime_data_sets = ["street", "outcomes", "stop-and-search"]
        for crime in data_sets:
            if crime not in valid_crime_data_sets:
                err_msg = "Invalid crime data set entered"
                logger.error(err_msg)
                raise Exception(err_msg)

        return data_sets


