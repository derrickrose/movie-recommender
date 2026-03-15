from typing import Dict, List

from botocore.client import BaseClient
from typing import Union, Type, Any

from typing import Optional, TypedDict
from pandas import DataFrame

class EnvironmentVariables(TypedDict):
    TEMP_FOLDERS: Optional[str]
    ATHENA_QUERY_RESULTS: Optional[str]
    VALID_TARGET_FILE_EXTENSIONS: Optional[str]
    DATAFRAME_ROWS_LIMIT: Optional[str]
    QUERY_FILTERING_ATTRIBUTES: Optional[list[str]]
    ATTRIBUTES_TO_ADD_ON_FILE_NAME: Optional[str]

class InvalidAttributeException(Exception):
    def __init__(self, message: str): ...

class MissingAttributeException(Exception):
    def __init__(self, message: str): ...

class Converter:
    df: DataFrame

    def __init__(self, df: DataFrame): ...
    def process(self, delimiter=None) -> bytes | str: ...

class JsonConverter(Converter):
    def __init__(self, data: DataFrame): ...
    def process(self, delimiter=None) -> str: ...

class CsvConverter(Converter):
    def __init__(self, data: DataFrame): ...
    def process(self, delimiter=None) -> bytes: ...

class ConverterConf(TypedDict):
    converter: Union[Type[CsvConverter], Type[JsonConverter]]
    is_delimiter_mandatory: bool

CONVERTER_STRATEGY: dict[str, ConverterConf]

def retrieve_environment() -> EnvironmentVariables: ...
def retrieve_attribute(
    event: dict, attribute: str, mandatory: bool = True
) -> str: ...
def validate_file_extension(
    file_extension: str, env: EnvironmentVariables
) -> str: ...
def handle_custom_exception(
    exception: (
        MissingAttributeException | InvalidAttributeException | TypeError
    ),
) -> dict: ...
def handle_boto_exception(exception: Exception) -> dict: ...
def handle_arrow_invalid(exception: Exception): ...
def build_file_name(
    query_filtering_attribute: str,
    file_name: str,
    file_extension: str,
    index: int,
) -> str: ...
def build_file_key(
    source_folder: str, new_file_name: str, env: EnvironmentVariables
) -> str: ...
def retrieve_parquet_files(
    event: dict, attribute: str, mandatory: bool = True
) -> list[dict[str, str]]: ...

class LambdaExtractionParquetToFlat(object):
    env: Dict[str, Any]
    event: Dict[str, Any]
    s3_client: BaseClient
    bucket_name: str
    prefix: str
    parquet_files: list[dict[str, str]]
    query_filtering_attribute_label: str
    query_filtering_attribute_value: str
    file_name: str
    file_extension: str
    converter_conf: Dict[str, Any]
    delimiter: str

    def __init__(
        self, event: Dict[str, Any], s3_client: BaseClient
    ) -> None: ...
    def retrieve_parameters(self) -> None: ...
    def read_parquet(self, object_key: str) -> DataFrame: ...
    def read_all_parquet_files(
        self, keys: List[Dict[str, str]]
    ) -> DataFrame: ...
    def to_smaller_dataframes(self, df: DataFrame) -> List[DataFrame]: ...
    def convert_to_flat(self, df: DataFrame) -> Union[bytes, str]: ...
    def build_new_key(self, index: int) -> str: ...
    def upload_to_s3(self, content: Union[bytes, str], key: str) -> None: ...
    def upload_all(self, flats: List[Union[bytes, str]]) -> dict[str, Any]: ...
    def execute(self) -> Dict[str, str]: ...

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, str]: ...
