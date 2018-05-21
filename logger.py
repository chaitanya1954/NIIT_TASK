import logging
import os
from logging.handlers import RotatingFileHandler

# Log handler
logLoc = os.getcwd()
dataCrawlLogger = logging.getLogger("DataCrawl")
dataCrawlLogger.setLevel(logging.DEBUG)
# add a rotating handler
handler = RotatingFileHandler(os.path.join(logLoc, "DataCrawl.log"),
                              maxBytes=300000000, backupCount=15)
handler.setLevel(logging.DEBUG)
fmtr = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                         datefmt="%m/%d/%Y %I:%M:%S %p")
handler.setFormatter(fmtr)
dataCrawlLogger.addHandler(handler)