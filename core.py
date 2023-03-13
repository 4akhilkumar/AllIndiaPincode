"""
This python script is used to load the data from the csv file,
setup the things and return the data
"""
import ast
import configparser
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Union

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from validations import validate_pincode


VERSION = 1
API_ENDPOINT = f"/api/v{VERSION}/"

# CONFIG FILE
config = configparser.ConfigParser()
config.read('config.ini')
CSV_FILE = config["file_data"]["csv_file"]
ENCODING = config["file_data"]["encoding"]
LOG_FILE = config["logging"]["log_file"]
MAX_BYTES = int(config["logging"]["max_bytes"])
LOG_LEVEL = config["logging"]["log_level"]
BACKUP_COUNT = int(config["logging"]["backup_count"])
REQUIRED_FIELDS_STR = config["setup_things"]["required_columns"]
REQUIRED_FIELDS = ast.literal_eval(REQUIRED_FIELDS_STR)
DATATYPE_RULE_STR = config["setup_things"]["datatype_rule"]
DATATYPE_RULE = ast.literal_eval(DATATYPE_RULE_STR)
REGEX_RULE_STR = config["setup_things"]["regex_rule"]
REGEX_RULE = ast.literal_eval(REGEX_RULE_STR)
FORMAT_RULE_STR = config["setup_things"]["format_rule"]
FORMAT_RULE = ast.literal_eval(FORMAT_RULE_STR)

# Logging Section
# configure the log format
LOG_META_ATTRS = '%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d \
- %(message)s'
FORMATTER = logging.Formatter(LOG_META_ATTRS, datefmt="%d-%b-%y %H:%M:%S")

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# ROTATING FILE HANDLER
rotating_handler = RotatingFileHandler(filename=LOG_FILE,
                                        maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT)
rotating_handler.setFormatter(FORMATTER)
logger.addHandler(rotating_handler)


class LoadData:
    """
    Load the data from the csv file, Setup the things and return the data
    """
    def __init__(self, csv_data: str):
        """
        Initialize the class
        """
        self.csv_data = csv_data
        self.dataframe = None

    def read_csv(self, cols: list = None) -> pd.DataFrame:
        """
        Read the csv file and return the dataframe

        Args:
            cols (list, optional): List of columns to read from the csv file. Defaults to None.

        Returns:
            pd.DataFrame: Dataframe of the csv file
        """
        try:
            self.dataframe = pd.read_csv(self.csv_data, usecols=cols,
                                    encoding=ENCODING, low_memory=False)
        except Exception as error:
            logger.error("Error %s occured while reading the csv file", error)
            raise HTTPException(status_code=500, detail="Internal Server Error") from error
        logger.info("%s File Loaded", self.csv_data)

    def change_datatype(self, datatype_rule: dict = None):
        """
        Change the datatype of the column in the dataframe
        """
        if not datatype_rule is None:
            datatype_class = {
                "string": str,
                "int": int,
                "float": float,
                "bool": bool
            }
            for column, datatype in datatype_rule.items():
                self.dataframe[column] = self.dataframe[column].astype(datatype_class[datatype])
                logger.info("Datatype of %s changed to %s", column, datatype)

    def regex_on_column(self, regex_rule: dict = None):
        """
        Apply regex on the column

        Args:
            regex_rule (dict, optional): Regex rule for the dataframe. Defaults to None.
        """
        if not regex_rule is None:
            for column, regex in regex_rule.items():
                self.dataframe[column] = self.dataframe[column].str.replace(regex, "", regex=True)
                logger.info("Regex %s applied on %s", regex, column)

    def format_csv_data(self, format_rule: dict = None):
        """
        Format the csv data

        Args:
            format_rule (dict, optional): Format rule for the dataframe. Defaults to None.
        """
        if not format_rule is None:
            string_methods = {
                "Title": str.title,
                "Upper": str.upper,
                "Lower": str.lower,
                "Capitalize": str.capitalize
            }
            for column, method in format_rule.items():
                self.dataframe[column] = self.dataframe[column].apply(string_methods[method])
                logger.info("Format %s applied on %s", method, column)

    def filter_by_pincode(self, pincode: int) -> list:
        """
        Filter the dataframe by pincode

        Args:
            pincode (int): Pincode to filter the dataframe

        Returns:
            list: List of the filtered dataframe
        """
        tmp_dataframe = self.dataframe.copy()
        tmp_dataframe = tmp_dataframe[tmp_dataframe["Pincode"] == pincode]
        if tmp_dataframe.empty:
            invalid_msg = f"Pincode {pincode} not found"
            logger.info(invalid_msg)
            raise HTTPException(status_code=404, detail=invalid_msg)
        return JSONResponse(status_code=200, content=tmp_dataframe.to_dict(orient="records"))

    def setup_things(self):
        """
        Setup the things
        """
        # Read the csv file
        self.read_csv(cols=REQUIRED_FIELDS)

        # Change the datatype of the column
        self.change_datatype(datatype_rule = DATATYPE_RULE)

        # Apply Regex on Column
        self.regex_on_column(regex_rule = REGEX_RULE)

        # Format the data
        self.format_csv_data(format_rule = FORMAT_RULE)


# On startup event,
# Load the data from the csv file, Change the datatype of the column,
# Apply Regex on Column, Format the data
class MySingleton:
    """
    Singleton class - Load the data from the csv file only once
    and return the instance
    """
    instance = None

    def __new__(cls):
        """
        Create the instance of the class if not created

        Returns:
            cls: Instance of the class
        """
        if cls.instance is None:
            cls.instance = LoadData(CSV_FILE)
            cls.instance.setup_things()
        return cls.instance

    def filter_by_pincode(self, pincode: int):
        """
        Filter the dataframe by pincode

        Args:
            pincode (int): Pincode to filter the dataframe
        """
        return self.instance.filter_by_pincode(pincode)


load_data = MySingleton()

app = FastAPI()

@app.get(f"{API_ENDPOINT}")
async def pincode_api(pincode: Optional[Union[str, int]] = None):
    """
    API to get the data by pincode

    API endpoint URL:
        Localhost: http://localhost:<PORT>/api/v1/?pincode=<PINCODE>

    Args:
        pincode (Optional) (str, int): Pincode to filter the dataframe. Defaults to None.
    """
    if pincode is None:
        logger.info("Pincode parameter is required.")
        raise HTTPException(status_code=400, detail="Pincode parameter is required.")

    if pincode is None or (isinstance(pincode, str) and pincode == ""):
        logger.info("Pincode should not be empty.")
        raise HTTPException(status_code=400, detail="Pincode should not be empty.")

    is_pincode_valid = validate_pincode(pincode)
    if is_pincode_valid['status'] is False:
        logger.info(is_pincode_valid['content'])
        raise HTTPException(status_code=400, detail=is_pincode_valid['content'])

    response = load_data.filter_by_pincode(int(pincode))
    return response
