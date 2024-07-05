import sys
import json
import time
import schedule
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS

def get_ftp() -> FTP_TLS:
    
    FTPHOST = environ["FTPHOST"]    # FTP server hostname or IP address
    FTPUSER = environ["FTPUSER"]    # Username for the FTP server
    FTPPASS = environ["FTPPASS"]    # Password for the FTP server

    # return authentication of FTP
    # ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)
    ftp = FTP_TLS(FTPHOST, FTPUSER, FTPPASS)        # creates an instance of the FTP_TLS class from the ftplib module
    ftp.prot_p()                                    # method enables protection for the data connection used for file transfer
    return ftp                                      # returns the object ftp, which represents the established secure connection to the FTP server

def upload_to_ftp(ftp: FTP_TLS, file_source: Path):
    with open(file_source, "rb") as fp:
        ftp.storbinary(f"STOR {file_source.name}", fp)

def delete_file(file_source: str | Path):
    remove(file_source)

def read_csv(config: dict) -> pd.DataFrame:         # this function retrieves data from a CSV file located at a specified URL and returns the data as a pandas DtaFrame
    url = config["URL"]                             # dictionary containing configuration information for the CSV download. URL of the CSV file to be downloaded
    params = config["PARAMS"]                       # (optional) dictionary containing additional parameters to be passed to the pandas.read_csv() function
    return pd.read_csv(url, **params)               # unpacked using the double star (**) to pass them as keyword arguements.

def pipeline():
     with open("config.json", "rb") as fp:           # loads the configuration from a JSON file named "config.json"
        config = json.load(fp)

     ftp = get_ftp()

     for source_name, source_config in config.items():       # creates a for loop to iterate the key value pairs of the config dictionary
        file_name = Path(source_name + ".CSV")                    # creates a new Path object named file_name using the source_name from the dictionary
        df = read_csv(source_config)                        # reads the CSV data from the specified URL using read_csv()
        df.to_csv(file_name, index=False)                   # saves the data as a CSV file with the source name, excluding row index being included in the save CSV file

        print(f"File {file_name} has been downloaded")

        upload_to_ftp(ftp, file_name)
        print(f"File {file_name} has been uploaded")


        delete_file(file_name)
        print(f"File {file_name} has been deleted")


if __name__=="__main__":

    param = sys.argv[1]

    if param=="manual":
        pipeline()
    elif param=="schedule":
        schedule.every().day.at("17:11").do(pipeline)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        print("Invalid parameter. App will not run")