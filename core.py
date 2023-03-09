"""
This python script is used to load the data from the csv file, some data cleaning
and return the data in the form of API
"""
import configparser
from typing import Optional, Union

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from validation import validate_pincode


VERSION = 1
API_ENDPOINT = f"/api/v{VERSION}/"

# CONFIG FILE
config = configparser.ConfigParser()
config.read('config.ini')
CSV_FILE = config["file_data"]["csv_file"]


class LoadData:
    """
    Load the data from the csv file
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
        self.dataframe = pd.read_csv(self.csv_data, usecols=cols,
                                    encoding='iso-8859-1', low_memory=False)
        print("CSV File Loaded")

    def change_datatype(self, datatype_rule: dict = None):
        """
        Change the datatype of the column in the dataframe
        """
        datatype_class = {
            "string": str,
            "int": int,
            "float": float,
            "bool": bool
        }
        for column, datatype in datatype_rule.items():
            self.dataframe[column] = self.dataframe[column].astype(datatype_class[datatype])
        print("Datatype Changed")

    def regex_on_column(self, regex_rule: dict = None):
        """
        Apply regex on the column

        Args:
            regex_rule (dict, optional): Regex rule for the dataframe. Defaults to None.
        """
        for column, regex in regex_rule.items():
            self.dataframe[column] = self.dataframe[column].str.replace(regex, "", regex=True)
        print("Regex Applied")

    def format_csv_data(self, format_rule: dict = None):
        """
        Format the csv data

        Args:
            format_rule (dict, optional): Format rule for the dataframe. Defaults to None.
        """
        string_methods = {
            "Title": str.title,
            "Upper": str.upper,
            "Lower": str.lower,
            "Capitalize": str.capitalize
        }
        for column, method in format_rule.items():
            self.dataframe[column] = self.dataframe[column].apply(string_methods[method])
        print("Data Formatted")

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
            raise HTTPException(status_code=404, detail=invalid_msg)
        return JSONResponse(status_code=200, content=tmp_dataframe.to_dict(orient="records"))

    def setup_things(self):
        """
        Setup the things
        """
        self.read_csv(
            cols = ["Office Name", "Pincode", "District", "StateName"]
        )

        # Change the datatype of the column
        datatype_rule = {
            # "Coulmn Name": "Datatype" E.g. String, Int, Float, Bool
            "District": "string",
        }
        self.change_datatype(datatype_rule)

        # Apply Regex on Column
        regex_rule = {
            # "Coulmn Name": "Regex Pattern"
            "Office Name": r"\s*(S\.O|B\.O| SO| BO)\b"
        }
        self.regex_on_column(regex_rule)

        # Format the data
        format_rule = {
            # "Coulmn Name": "Format" E.g. Title, Upper, Lower, Capitalize
            "District": "Title"
        }
        self.format_csv_data(format_rule)


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
        raise HTTPException(status_code=400, detail="Pincode parameter is required.")

    if pincode is None or (isinstance(pincode, str) and pincode == ""):
        raise HTTPException(status_code=400, detail="Pincode should not be empty.")

    is_pincode_valid = validate_pincode(pincode)
    if is_pincode_valid['status'] is False:
        raise HTTPException(status_code=400, detail=is_pincode_valid['content'])

    response = load_data.filter_by_pincode(int(pincode))
    return response
