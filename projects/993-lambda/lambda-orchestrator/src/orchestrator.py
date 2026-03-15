import abc
import logging
import os
from io import BytesIO
from pathlib import PurePath
from typing import Optional, Type, TypedDict, Any

import boto3
import pandas as pd
from pandas import DataFrame
from botocore.exceptions import SSLError, ClientError
from pyarrow import ArrowInvalid
from pythonjsonlogger import json as jsonlogger

logger = logging.getLogger("lambda-extraction-parquets-to-flat")
logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logging.StreamHandler())

SESSION = boto3.Session()
S3_CLIENT = SESSION.client("s3")


class EnvironmentVariables(TypedDict):
    TEMP_FOLDERS: Optional[str]
    ATHENA_QUERY_RESULTS: Optional[str]
    VALID_TARGET_FILE_EXTENSIONS: Optional[str]
    DATAFRAME_ROWS_LIMIT: Optional[str]
    QUERY_FILTERING_ATTRIBUTES: Optional[list[str]]
    ATTRIBUTES_TO_ADD_ON_FILE_NAME: Optional[str]


class MissingAttributeException(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidAttributeException(Exception):
    def __init__(self, message):
        super().__init__(message)


def retrieve_environment() -> EnvironmentVariables:
    query_filtering_attributes_raw = os.getenv("QUERY_FILTERING_ATTRIBUTES")
    return {
        "TEMP_FOLDERS": os.getenv("TEMP_FOLDERS"),
        "ATHENA_QUERY_RESULTS": os.getenv("ATHENA_QUERY_RESULTS"),
        "VALID_TARGET_FILE_EXTENSIONS": os.getenv(
            "VALID_TARGET_FILE_EXTENSIONS"
        ),
        "DATAFRAME_ROWS_LIMIT": os.getenv("DATAFRAME_ROWS_LIMIT"),
        "QUERY_FILTERING_ATTRIBUTES": (
            query_filtering_attributes_raw.split(",")
            if query_filtering_attributes_raw
            else None
        ),
        "ATTRIBUTES_TO_ADD_ON_FILE_NAME": os.getenv(
            "ATTRIBUTES_TO_ADD_ON_FILE_NAME"
        ),
    }


def retrieve_attribute(event, attribute: str, mandatory: bool = True) -> str:
    attribute_value = event.get(attribute, "")
    if mandatory and not attribute_value:
        raise MissingAttributeException(
            f"Le champs '{attribute}' est manquant, merci de le renseigner."
        )
    return attribute_value


def retrieve_query_filtering_attribute(
    event, environment
) -> Optional[tuple[str, str]]:
    query_filtering_attributes_message = " ou ".join(
        [
            f"'{attribute}'"
            for attribute in environment["QUERY_FILTERING_ATTRIBUTES"]
        ]
    )
    for value in environment["QUERY_FILTERING_ATTRIBUTES"]:
        query_filtering_attribute = event.get(value, None)
        if query_filtering_attribute:
            return value, query_filtering_attribute
    raise MissingAttributeException(
        f"Un des champs {query_filtering_attributes_message} "
        f"doit être présent, merci de le renseigner."
    )


def retrieve_parquet_files(
    event, attribute: str, mandatory: bool = True
) -> list[dict[str, str]]:
    attribute_value = event.get(attribute, [])
    if mandatory and not attribute_value:
        raise MissingAttributeException(
            f"Le champs '{attribute}' est manquant, merci de le renseigner."
        )
    return attribute_value


def validate_file_extension(file_extension: str, env: dict) -> str:
    if file_extension not in env["VALID_TARGET_FILE_EXTENSIONS"]:
        raise InvalidAttributeException(
            "Valeure invalide du champs 'file_extension', "
            "valeure attendue 'csv ou json'."
        )
    return file_extension.lower()


def handle_custom_exception(
    exception: (
        MissingAttributeException | InvalidAttributeException | TypeError
    ),
):
    cause = str(exception)
    if isinstance(exception, TypeError) and cause.__contains__(
        '"delimiter" must be a 1-character string'
    ):
        cause = (
            "Le délimiteur doit être un seul caractère, "
            "merci de renseigner un délimiteur valide."
        )
    elif isinstance(exception, TypeError):
        raise exception
    return {
        "StatusCode": "422",
        "Error": type(exception).__name__,
        "Cause": cause,
    }


def handle_boto_exception(exception):
    error_message = str(exception)
    if error_message.lower().__contains__(
        "404"
    ) and error_message.lower().__contains__("not found"):
        error_message = (
            "Un des fichiers à convertir n'existe pas "
            "à l'endroit mentionné, merci de renseigner un fichier existant."
        )
    else:
        error_message = (
            "La requête n'a pas aboutie "
            "suite à un incident technique, "
            "merci de la renouveler ultérieurement."
        )
    return {
        "StatusCode": "422",
        "Error": type(exception).__name__,
        "Cause": error_message,
    }


def build_file_name(
    query_filtering_attribute: str,
    file_name: str,
    file_extension: str,
    index: int,
    environment: dict,
    query_filtering_attribute_label: str,
) -> str:
    new_name = (
        f"{query_filtering_attribute}_{file_name}"
        if query_filtering_attribute_label
        in environment["ATTRIBUTES_TO_ADD_ON_FILE_NAME"]
        else file_name
    )
    new_name = f"{new_name}_{index}" if index else new_name
    return f"{new_name}.{file_extension}"


def build_file_key(
    source_folder: str,
    new_file_name: str,
    env: EnvironmentVariables,
) -> str:
    destination_folder = (
        source_folder.replace(env["ATHENA_QUERY_RESULTS"], env["TEMP_FOLDERS"])
        if (env["ATHENA_QUERY_RESULTS"] and env["TEMP_FOLDERS"])
        else source_folder
    )
    return PurePath(destination_folder, new_file_name).as_posix()


class LambdaOrchestrator(object):
    def __init__(self, event, s3_client=None):
        self.env = retrieve_environment()
        self.event = event
        self.s3_client = s3_client or S3_CLIENT

        self.bucket_name = None
        self.prefix = None
        self.parquet_files = None
        self.query_filtering_attribute_label = None
        self.query_filtering_attribute_value = None
        self.file_name = None
        self.file_extension = None
        self.converter_conf = None
        self.delimiter = None

    def retrieve_parameters(self) -> None:
        self.bucket_name = retrieve_attribute(self.event, "bucket_name")
        self.prefix = retrieve_attribute(self.event, "prefix")
        self.parquet_files = retrieve_parquet_files(
            self.event, "parquet_files"
        )
        (
            self.query_filtering_attribute_label,
            self.query_filtering_attribute_value,
        ) = retrieve_query_filtering_attribute(self.event, self.env)
        self.file_name = retrieve_attribute(self.event, "file_name")
        self.file_extension = validate_file_extension(
            retrieve_attribute(self.event, "file_extension"), self.env
        )
        self.converter_conf = CONVERTER_STRATEGY[self.file_extension]
        self.delimiter = retrieve_attribute(
            self.event,
            "delimiter",
            self.converter_conf["is_delimiter_mandatory"],
        )
        logger.info(
            f"Les paramètres récupérés en entrée ==> \n"
            f"bucket_name: {self.bucket_name}, \n"
            f"parquet_files: {self.parquet_files}, \n"
            f"query_filtering_attribute_value : "
            f"{self.query_filtering_attribute_value}, \n"
            f"file_name: {self.file_name}, \n"
            f"file_extension: {self.file_extension}, \n"
            f"delimiter: {self.delimiter}, \n"
        )


    def execute(self) -> dict[str, str]:
        try:
            # Collect and validate all parameters
            self.retrieve_parameters()

            # Read all parquet files and concat them to one big dataframe
            big_df = self.read_all_parquet_files(self.parquet_files)

            # Divide dataframe to smaller dataframes
            # with a limit of 1M lines each
            dfs = self.to_smaller_dataframes(big_df)

            # Convert all dataframes to flat (CSV or Json)
            flats = list(map(lambda x: self.convert_to_flat(x), dfs))

            # Upload all flat files to s3
            upload_response = self.upload_all(flats)
        except (
            MissingAttributeException,
            InvalidAttributeException,
            TypeError,
        ) as cust:
            return handle_custom_exception(cust)
        except (SSLError, ClientError) as boto_err:
            return handle_boto_exception(boto_err)
        except ArrowInvalid as arr:
            return handle_arrow_invalid(arr)

        return {
            "StatusCode": "200",
            "Message": upload_response["message"],
            "ConvertedFiles": upload_response["files"],
        }


def lambda_handler(event, context):
    lambda_convertor = LambdaOrchestrator(event)
    return lambda_convertor.execute()
