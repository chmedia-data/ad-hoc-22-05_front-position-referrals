import os, json, re, logging
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq

import dotenv; dotenv.load_dotenv(".env")

logger = logging.getLogger()

class BigQuery:
    def __init__(self):
        self.credentials = None
        self.client = None
        return

    def _get_client(self):
        if self.credentials is None:
            self.credentials = self._get_credentials()
            self.client = bigquery.Client(credentials=self.credentials)

        if self.client is None:
            self.client = bigquery.Client(credentials=self.credentials)

        return self.client

    def insert(self,table,rows_to_insert):

        client = self._get_client()

        if os.environ.get("TEST","false").lower()=="true":
            logger.debug("replace bq table with dev space")
            table = table.lower().replace("chmedia.","chmedia_dev.")+os.environ["TEST_SUFFIX"]

        errors = client.insert_rows_json(table, rows_to_insert)
        if len(errors)>0:
            raise Exception("Google BigQuery Error:\n\n"+json.dumps(errors,indent=2))

        return len(rows_to_insert)

    def getQueryDF(self,query):
        if self.credentials is None:
            self.credentials = self._get_credentials()

        df = pandas_gbq.read_gbq(
            query,
            credentials=self.credentials,
            project_id="trim-mechanism-126723",
            progress_bar_type=None
        )
        return df

    def execute(self,query):

        client = self._get_client()

        if os.environ.get("TEST","false").lower()=="true":
            logger.debug("replace bq table with dev space")

            query = re.sub(
                r"insert into chmedia.(\w+)\b",
                r"insert into chmedia_dev.\1"+os.environ["TEST_SUFFIX"],
                query,
                re.IGNORECASE
            )

        result = client.query(query).result()

        return result

    def _get_credentials(self):

        service_account_info = {k.replace("GOOGLE_","").lower():v for k,v in os.environ.items() if k.startswith("GOOGLE_")}
        service_account_info["auth_uri"] = "https://accounts.google.com/o/oauth2/auth"
        service_account_info["token_uri"] = "https://oauth2.googleapis.com/token"
        service_account_info["auth_provider_x509_cert_url"] = "https://www.googleapis.com/oauth2/v1/certs",

        credentials = service_account.Credentials.from_service_account_info(service_account_info)

        return credentials
