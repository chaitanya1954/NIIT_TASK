import csv
import config
import os
import schedule
import time
import traceback
import pandas as pd
import urllib.request as urllib
from datetime import datetime
from logger import dataCrawlLogger

download_link = config.downloadLink


class Downloader:
    """
    This class downloads the latest file from the
    links present in the config file
    """
    def __init__(self, soft_name, download_link):
        self.soft_name = soft_name
        self.download_link = download_link

    def download(self):
        """
        This method is responsible for parsing the url
        and downloading the file in the processing folder.
        :return: downloaded file
        """
        try:
            download_loc = os.path.join(os.getcwd(), 'processing')
            if not os.path.exists(download_loc):
                os.makedirs(download_loc)

            soft_name = self.soft_name.replace(' ', '')
            # u1 = urllib.urlopen(self.download_link)
            # print('Downloading.....---->', soft_name,
            #       '-----------------', self.download_link)
            output_file = os.path.join(download_loc, soft_name)
            # local_file = open(output_file, 'wb')
            # local_file.write(u1.read())
            # local_file.close()
            # u1.close()
            return output_file
        except Exception as e:
            dataCrawlLogger.error(traceback.format_exc())


class FindLastRetrievedData:
    """
    This class is for retrieving the last updated data which is present in the
    processed folder.
    """
    def __init__(self, file_name):
        self.file_name = file_name

    def find_latest_file(self):
        """
        This method verifies that the processed directory is not empty
        and finds the latest file, the latest file is assumed to have the
        same name as xlsx file with a different extension(csv).
        :return: method call to get_last_updated_data
        """
        processed_loc = os.path.join(os.getcwd(), 'processed')
        if not os.path.exists(processed_loc):
            os.makedirs(processed_loc)
            return
        if os.listdir(processed_loc) == []:
            return
        latest_file = os.path.join(processed_loc, os.path.basename(self.file_name).split('.')[0] + '.csv')
        return self.get_last_updated_data(latest_file)

    def get_last_updated_data(self, latest_file):
        """
        This method finds the last updated date of the previously processed file.
        :param latest_file: last updated file in the processed directory
        :return: datetime object of the last updated date
        """
        with open(latest_file) as f:
            for line in csv.reader(f):
                pass
            return datetime.strptime(line[0], '%m/%d/%Y')


class ParseData:
    """
    This class parses the required data from xlsx file
    inside the processing directory and saves it to the
    csv file inside the processed directory.
    """
    def __init__(self, file_name, last_updated_date):
        self.file_name = file_name
        self.last_updated_date = last_updated_date
        self.months = {
            'Jan': 1, 'Feb': 2, 'Mar': 3,
            'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9,
            'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        self.header_24 = [
            'Date',
            'BCB_Commercial_Exports_Total',
            'BCB_Commercial_Exports_Advances_on_Contracts',
            'BCB_Commercial_Exports_Payment_Advance',
            'BCB_Commercial_Exports_Others',
            'BCB_Commercial_Imports',
            'BCB_Commercial_Balance',
            'BCB_Financial_Purchases',
            'BCB_Financial_Sales',
            'BCB_Financial_Balance',
            'BCB_Balance'
        ]
        self.header_26 = [
            'Date',
            'BCB_FX_Position'
        ]

    def file_selector(self):
        if '24' in self.file_name:
            return self.parse_24_file()
        else:
            return self.parse_26_file()

    def parse_24_file(self):
        """
        This method parses ie5-24i.xlsx file
        :return: None
        """
        data = pd.read_excel(
            self.file_name,
            skiprows=4,
            skipfooter=9
        )
        data.dropna(how='all')
        year, month, date, correct_data = 0, 0, 0, []
        for i in data.iterrows():
            x, y = i[1]['Unnamed: 0'], i[1]['Unnamed: 1']
            if isinstance(x, int) and x > 999:
                year = x
            if isinstance(y, str) and y in self.months:
                month = y
            if isinstance(y, int) and 0 < y < 32:
                date = y
            if year > 0 and date > 0 and month in self.months:
                current_date = datetime.strptime(
                    '{}/{}/{}'.format(year, self.months[month], date),
                    '%Y/%m/%d'
                )
                if current_date > self.last_updated_date:
                    data_row = [x for x in i[1].values]
                    data_row = data_row[1:]
                    data_row[0] = current_date.date()
                    correct_data.append(data_row)
        self.save_data_to_csv(correct_data, 'ie5-24_output.csv', self.header_24)

    def parse_26_file(self):
        """
        This method parses ie5-26i.xlsx file
        :return: None
        """
        data = pd.read_excel(
            self.file_name,
            skiprows=9,
            skipfooter=5
        )
        data.dropna(how='all')
        year, month, correct_data = 0, 0, []
        for i in data.iterrows():
            x, y = i[1]['Unnamed: 0'], i[1]['Unnamed: 1']
            if isinstance(x, float) and x > 999:
                year = x
            if isinstance(y, str) and y in self.months:
                month = y
            if year > 0 and month in self.months:
                current_date = datetime.strptime(
                    '{}/{}/{}'.format(int(year), self.months[month], 1),
                    '%Y/%m/%d'
                )
                if current_date > self.last_updated_date:
                    data_row = [x for x in i[1].values]
                    data_row = data_row[1:]
                    data_row[0] = current_date.date()
                    correct_data.append(data_row)
        self.save_data_to_csv(correct_data, 'ie5-26_output.csv', self.header_26)

    def save_data_to_csv(self, data, output_file, header):
        """
        This method just saves the parsed data into the csv file
            :param data: list of list
            :param output_file: name of the output file
            :param header: header based on file
            :return: Just creates an output file, returns None
        """
        df = pd.DataFrame(data)
        df.to_csv(
            os.path.join(
                os.getcwd(),
                'processed',
                output_file
            ),
            index=False,
            header=header
        )


class Archiver:
    """
    This class archives the processed file from the processing directory
    to archived directory. Although the functionality is commented as
    we do need to moves the files for testing purpose.
    """
    def __init__(self, file_name):
        self.file_name = file_name
        self.archived_loc = os.path.join(os.getcwd(), 'archived')
        if not os.path.exists(self.archived_loc):
            os.makedirs(self.archived_loc)

    def archive_processed_file(self):
        """
        Archives the files from processing folder to archived folder
        :return: None
        """
        # os.rename(
        #     self.file_name,
        #     os.path.join(os.getcwd(), 'archived', os.path.basename(self.file_name))
        # )
        pass


def main():
    """
    Main callable function for
    Downloading the excel file 
    and Parse the Excel
    """
    for link in download_link:
        file = link.split('/')[-1]

        download_inst = Downloader(file, link)
        downloaded_file = download_inst.download()

        last_data = FindLastRetrievedData(downloaded_file)
        last_updated_date = last_data.find_latest_file()

        parse = ParseData(downloaded_file, last_updated_date)
        parse.file_selector()

        archive = Archiver(downloaded_file)
        archive.archive_processed_file()


if __name__ == "__main__":

    schedule.every(1).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)