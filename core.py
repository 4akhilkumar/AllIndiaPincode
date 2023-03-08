"""
This python script is used to load the data from the csv file, some data cleaning
and return the data in the form of API
"""
import pandas as pd
from fastapi import FastAPI, HTTPException

LOAD_DATA = None
VERSION = 1
API_ENDPOINT = f"/api/v{VERSION}/"

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

    def filter_by_pincode(self, pincode: int) -> pd.DataFrame:
        """
        Filter the dataframe by pincode

        Args:
            pincode (int): Pincode to filter the dataframe

        Returns:
            pd.DataFrame: Filtered dataframe
        """
        tmp_dataframe = self.dataframe.copy()
        tmp_dataframe = tmp_dataframe[tmp_dataframe["Pincode"] == pincode]
        if tmp_dataframe.empty:
            invalid_msg = f"Pincode {pincode} not found"
            raise HTTPException(status_code=404, detail=invalid_msg)
        return tmp_dataframe.to_dict(orient="records")

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


app = FastAPI()

@app.on_event("startup")
async def startup_event():
    """
    On startup event,
    Load the data from the csv file, Change the datatype of the column,
    Apply Regex on Column, Format the data
    """
    global LOAD_DATA
    LOAD_DATA = LoadData("Pincode_30052019.csv")
    LOAD_DATA.setup_things()


@app.get(f"{API_ENDPOINT}")
async def pincode_api(pincode: int | None):
    """
    API to get the data by pincode
    """
    response = LOAD_DATA.filter_by_pincode(pincode)
    return response

