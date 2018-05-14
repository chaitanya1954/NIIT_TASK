import logging
from logging.handlers import RotatingFileHandler
import os
import urllib2
import requests
import traceback
import time
import config
import pandas as pd
import xlwt
import time
import schedule


# Log handler
logLoc = os.getcwd()
dataCrawlLogger = logging.getLogger("DataCrawl")
dataCrawlLogger.setLevel(logging.DEBUG) 
# add a rotating handler
handler = RotatingFileHandler(os.path.join(logLoc,"DataCrawl.log"), maxBytes=300000000, backupCount=15)
handler.setLevel(logging.DEBUG)
fmtr = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
handler.setFormatter(fmtr)
dataCrawlLogger.addHandler(handler)
DownloadLink = config.downloadLink

def Downloader(softName, DownloadLink):
    """
    @Takes input as Filename and download link
    @retruns a downloadable file
    """
    try:
        downloadLoc = os.getcwd()
        softName = softName.replace(' ', '')
        u1 = urllib2.urlopen(DownloadLink)
        print('Downloading.....---->' + softName + '-----------------', DownloadLink)
        localFile = open(downloadLoc + '\\' + softName, 'wb')
        localFile.write(u1.read())
        localFile.close()
        u1.close()
    except Exception as e:
        dataCrawlLogger.error(traceback.format_exc())


def readExcelFile(fileName):
    """
    @Reads excel file from current directory
    @returns: An updated excel with required data
    """
    writer = pd.ExcelWriter("bcb_output_1_1.xlsx", engine='xlsxwriter')
    dateList,BCBCommercialEx,BCBCEAC,BCBCEPA,BCBCEO,BCBCI,BCBCB,BCBFP,BCBFS,BCBFB,BCBB = [],[],[],[],[],[],[],[],[],[],[]      
    excelPath = os.path.join(os.getcwd(),fileName)
 
    transactionExcel = pd.ExcelFile(excelPath)
    rows = transactionExcel.book.sheet_by_index(0).nrows
    workbook_dataframe = transactionExcel.parse('IE5-24I', skiprows=46,skip_footer=10)

    cnt = 0
    CntVal = 1
    colVal = 0
    for df_key,df_value in workbook_dataframe.items():
        try:
            cnt +=1
            if cnt>=3:
                colVal +=1
                dateCnt = 0
                eValSum = 0
                for index,eVal in enumerate(df_value):
                    dateCnt +=1
                    if CntVal==1:
                        dateList.append(dateCnt)
                        
                    eValSum +=eVal
                    
                    if colVal==1:
                        
                        BCBCommercialEx.append(eValSum)
                    elif colVal==2:
                        BCBCEAC.append(eValSum)
                    elif colVal==3:
                        BCBCEPA.append(eValSum)
                    elif colVal==4:
                        BCBCEO.append(eValSum)
                    elif colVal==5:
                        BCBCI.append(eValSum)
                    elif colVal==6:
                        BCBCB.append(eValSum)
                    elif colVal==7:
                        BCBFP.append(eValSum)
                    elif colVal==8:
                        BCBFS.append(eValSum)
                    elif colVal==9:
                        BCBFB.append(eValSum)
                    elif colVal==10:
                        BCBB.append(eValSum)
                CntVal +=1
        except Exception as e:
            dataCrawlLogger.error(traceback.format_exc())
  
    df = pd.DataFrame({'Date':dateList,'BCB_Commercial_Exports_Total':BCBCommercialEx,'BCB_Commercial_Exports_Advances_on_Contracts':BCBCEAC,
                       'BCB_Commercial_Exports_Payment_Advance':BCBCEPA,'BCB_Commercial_Exports_Others':BCBCEO,
                       'BCB_Commercial_Imports':BCBCI,'BCB_Commercial_Balance':BCBCB,'BCB_Financial_Purchases':BCBFP,
                       'BCB_Financial_Sales':BCBFS,'BCB_Financial_Balance':BCBFB,'BCB_Balance':BCBB})
    df.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    
def main():
    """
    Main callable function for
    Downloading the excel file 
    and Parse the Excel
    """
    fileName = DownloadLink.split('/')[-1]
    Downloader(fileName,DownloadLink)
    time.sleep(5)
    readExcelFile(fileName)  
    
if __name__ == "__main__":   


    schedule.every().day.at("19:59").do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
