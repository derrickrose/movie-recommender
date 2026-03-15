import os
import unittest
import io

import boto3
import pandas as pd
from pandas import DataFrame

from .conftest import (
    init_localstack,
    clean_localstack,
    region,
    local_host,
    list_bucket,
)

from src.orchestrator import LambdaExtractionParquetToFlat

S3_CLIENT = boto3.client(
    "s3",
    endpoint_url=local_host,
    aws_access_key_id="fake",
    aws_secret_access_key="fake",
    region_name=region,
    verify=False,
)


def numero_client_based_event():
    return {
        "prefix": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer",
        "bucket_name": "test-bucket",
        "numero_client": "1205706",
        "file_name": "EX014_extraction_data_sms_mail_contact_client",
    }


def parquet_to_json_event_OK():
    return {
        **numero_client_based_event(),
        "parquet_files": [
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/parquetfile2"
            },
        ],
        "file_extension": "json",
    }


def parquets_to_csv_event_OK():
    return {
        **numero_client_based_event(),
        "parquet_files": [
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/parquetfile"
            },
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/parquetfile2"
            },
        ],
        "file_extension": "csv",
        "delimiter": ";",
    }


def read_csv(bucket, key, client):
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_csv(io.BytesIO(data))


def read_json(bucket, key, client):
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_json(io.BytesIO(data))


def read_parquet(bucket, key, client) -> DataFrame:
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    return pd.read_parquet(io.BytesIO(data))


class TestLambdaExtractionParquetToFlat(unittest.TestCase):
    @classmethod
    def load_environment_variable(cls):
        os.environ["TEMP_FOLDERS"] = "temp_folders"
        os.environ["ATHENA_QUERY_RESULTS"] = "athena_query_results"
        os.environ["VALID_TARGET_FILE_EXTENSIONS"] = "csv,json"
        os.environ["LOG_LEVEL"] = "INFO"
        os.environ["DATAFRAME_ROWS_LIMIT"] = "1000000"
        os.environ["QUERY_FILTERING_ATTRIBUTES"] = (
            "periode_provision,numero_client"
        )
        os.environ["ATTRIBUTES_TO_ADD_ON_FILE_NAME"] = "numero_client"

    def test_environment_variable(self):
        self.load_environment_variable()
        assert os.getenv("TEMP_FOLDERS") == "temp_folders"

    def test_parquet_to_json_OK(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquet_to_json_event_OK()
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        object_key = "temp_folders/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/1205706_EX014_extraction_data_sms_mail_contact_client.json"
        try:
            assert response["StatusCode"] == "200"
            assert response.get("Cause", None) is None
            assert str(object_key) in list_bucket(
                S3_CLIENT, prefix="temp_folders"
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_OK(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        parquet_1_rows = len(
            read_parquet(
                bucket="test-bucket",
                key=event["parquet_files"][0]["Key"],
                client=S3_CLIENT,
            )
        )
        parquet_2_rows = len(
            read_parquet(
                bucket="test-bucket",
                key=event["parquet_files"][1]["Key"],
                client=S3_CLIENT,
            )
        )
        input_total_rows = parquet_1_rows + parquet_2_rows
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()

        output_object_key = "temp_folders/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/1205706_EX014_extraction_data_sms_mail_contact_client.csv"
        output_csv_rows = len(
            read_csv(
                bucket="test-bucket", key=output_object_key, client=S3_CLIENT
            )
        )
        try:
            assert response["StatusCode"] == "200"
            assert response.get("Cause", None) is None
            assert str(output_object_key) in list_bucket(
                S3_CLIENT, prefix="temp_folders"
            )
            assert output_csv_rows == input_total_rows
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_split_csv_OK(self):
        self.load_environment_variable()
        os.environ["DATAFRAME_ROWS_LIMIT"] = (
            "2"  # set maximum rows in each output files to 2
        )
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        parquet_1_rows = len(
            read_parquet(
                bucket="test-bucket",
                key=event["parquet_files"][0]["Key"],
                client=S3_CLIENT,
            )
        )
        parquet_2_rows = len(
            read_parquet(
                bucket="test-bucket",
                key=event["parquet_files"][1]["Key"],
                client=S3_CLIENT,
            )
        )
        input_total_rows = parquet_1_rows + parquet_2_rows
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        assert response["StatusCode"] == "200"
        assert response.get("Cause", None) is None
        expected_output_keys = [
            "temp_folders/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/1205706_EX014_extraction_data_sms_mail_contact_client.csv",
            "temp_folders/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/1205706_EX014_extraction_data_sms_mail_contact_client_1.csv",
            "temp_folders/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/1205706_EX014_extraction_data_sms_mail_contact_client_2.csv",
        ]
        actual_output_keys = list_bucket(S3_CLIENT, prefix="temp_folders")
        actual_output_csv_rows = len(
            read_csv(
                bucket="test-bucket",
                key=expected_output_keys[0],
                client=S3_CLIENT,
            )
        )
        actual_output_csv_1_rows = len(
            read_csv(
                bucket="test-bucket",
                key=expected_output_keys[0],
                client=S3_CLIENT,
            )
        )
        actual_output_csv_2_rows = len(
            read_csv(
                bucket="test-bucket",
                key=expected_output_keys[0],
                client=S3_CLIENT,
            )
        )
        try:
            for k in expected_output_keys:
                assert k in actual_output_keys
            assert 2 == actual_output_csv_rows
            assert 2 == actual_output_csv_1_rows
            assert 2 == actual_output_csv_2_rows
            assert (
                input_total_rows
                == actual_output_csv_rows
                + actual_output_csv_1_rows
                + actual_output_csv_2_rows
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_delimiter_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("delimiter")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'delimiter' est manquant, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_bucket_name_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("bucket_name")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'bucket_name' est manquant, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_numero_client_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("numero_client")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Un des champs 'periode_provision' ou 'numero_client' doit être présent, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_file_extension_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("file_extension")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'file_extension' est manquant, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_without_parquet_files_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("parquet_files")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'parquet_files' est manquant, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_wrong_file_extension_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event["file_extension"] = "orc"
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Valeure invalide du champs 'file_extension', valeure attendue 'csv ou json'."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_file_name_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("file_name")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'file_name' est manquant, merci de le renseigner."
            )

        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_without_prefix_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event.pop("prefix")
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le champs 'prefix' est manquant, merci de le renseigner."
            )
        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_two_parquet_files_to_csv_with_incorrect_delimiter_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event["delimiter"] = ";;"
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Le délimiteur doit être un seul caractère, merci de renseigner un délimiteur valide."
            )

        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_text_file_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event["parquet_files"] = [
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/txtfile"
            }
        ]
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Un des fichiers à convertir est corrompu ou n'est pas un fichier parquet, merci de renseigner un fichier valide."
            )

        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_empty_file_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event["parquet_files"] = [
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/emptyfile"
            }
        ]
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Un des fichiers à convertir est vide, merci de renseigner un fichier valide."
            )

        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)

    def test_file_not_found_KO(self):
        self.load_environment_variable()
        init_localstack(S3_CLIENT)
        event = parquets_to_csv_event_OK()  # 2 parquet files in the event
        event["parquet_files"] = [
            {
                "Key": "athena_query_results/Client1-2024-12-12T10:33:15.474Z/EX034_extraction_desicion_eer/a.parquet"
            }
        ]
        lambda_converter = LambdaExtractionParquetToFlat(event, S3_CLIENT)
        response = lambda_converter.execute()
        try:
            assert response["StatusCode"] == "422"
            assert (
                response.get("Cause", None)
                == "Un des fichiers à convertir n'existe pas à l'endroit mentionné, merci de renseigner un fichier existant."
            )

        except Exception as e:
            clean_localstack(S3_CLIENT)
            raise e
        clean_localstack(S3_CLIENT)


if __name__ == "__main__":
    unittest.main()
