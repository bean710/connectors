#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Oracle source module is responsible to fetch documents from Oracle."""

import asyncio
import csv
import json
import netrc
import os
from functools import cached_property, partial
from urllib.parse import quote, unquote, urlparse

import aiohttp

from asyncpg.exceptions._base import InternalClientError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

import aiofiles
import requests
import json

from connectors.es.sink import OP_INDEX
from connectors.source import BaseDataSource
from connectors.sources.generic_database import (
    DEFAULT_FETCH_SIZE,
    DEFAULT_RETRY_COUNT,
    Queries,
    configured_tables,
    fetch,
    is_wildcard,
    map_column_names,
)
from connectors.utils import convert_to_b64, iso_utc, parse_datetime_string

DEFAULT_PROTOCOL = "TCP"
DEFAULT_ORACLE_HOME = ""
SID = "sid"
SERVICE_NAME = "service_name"
ORACLE_FILE_URLS_FIELD = "_oracle_file_urls"
DEFAULT_HTTP_DOWNLOAD_CHUNK_SIZE = 1024 * 64
MAX_CHUNK_SIZE = 65536
EDMS_BASE_URL = "https://epaccwa.inl.gov/CMEWebAPI/api/docs/"
EDMS_URL_PATH = "/doc-file-content/1"


class OracleQueries(Queries):
    """Class contains methods which return query"""

    def ping(self):
        """Query to ping source"""
        return "SELECT 1+1 FROM DUAL"

    def all_tables(self, **kwargs):
        """Query to get all tables"""
        return (
            f"SELECT TABLE_NAME FROM all_tables where OWNER = UPPER('{kwargs['user']}')"
        )

    def table_primary_key(self, **kwargs):
        """Query to get the primary key"""
        [owner, table] = kwargs['table'].split('.')
        return f"SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = '{table}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = UPPER('{owner}') AND cons.owner = cols.owner ORDER BY cols.table_name, cols.position"

    def table_data(self, **kwargs):
        """Query to get the table data"""
        if 'timestamp' in kwargs and kwargs['timestamp'] is not None and 'updated_date_column' in kwargs and kwargs['updated_date_column'] is not None:
            timestamp = kwargs['timestamp']
            return f"SELECT * FROM {kwargs['table']} WHERE {kwargs['updated_date_column']} >= TO_DATE ('{timestamp}', 'YYYY-MM-DD HH24:MI:SS')"
        
        return f"SELECT * FROM {kwargs['table']}"

    def table_last_update_time(self, **kwargs):
        """Query to get the last update time of the table"""
        if 'updated_date_column' in kwargs and kwargs['updated_date_column'] is not None:
            return f"SELECT MAX({kwargs['updated_date_column']}) FROM {kwargs['table']}"

        return f"SELECT SCN_TO_TIMESTAMP(MAX(ora_rowscn)) from {kwargs['table']}"

    def table_data_count(self, **kwargs):
        """Query to get the number of rows in the table"""
        return f"SELECT COUNT(*) FROM {kwargs['table']}"

    def all_schemas(self):
        """Query to get all schemas of database"""
        pass  # Multiple schemas not supported in Oracle


class OracleClient:
    def __init__(
        self,
        host,
        port,
        user,
        password,
        connection_source,
        sid,
        service_name,
        tables,
        updated_date_column,
        primary_key_column,
        protocol,
        oracle_home,
        wallet_config,
        file_location_column,
        logger_,
        retry_count=DEFAULT_RETRY_COUNT,
        fetch_size=DEFAULT_FETCH_SIZE,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection_source = connection_source
        self.sid = sid
        self.service_name = service_name
        self.tables = tables
        self.updated_date_column = updated_date_column
        self.primary_key_column = primary_key_column
        self.protocol = protocol
        self.oracle_home = oracle_home
        self.wallet_config = wallet_config
        self.retry_count = retry_count
        self.fetch_size = fetch_size
        self.file_location_column = file_location_column

        self.connection = None
        self.queries = OracleQueries()
        self._logger = logger_

    def set_logger(self, logger_):
        self._logger = logger_

    def close(self):
        if self.connection is not None:
            self.connection.close()

    @cached_property
    def engine(self):
        """Create sync engine for oracle"""
        if self.connection_source == SID:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(SID={self.sid})))"
        else:
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL={self.protocol})(HOST={self.host})(PORT={self.port}))(CONNECT_DATA=(service_name={self.service_name})))"
        connection_string = (
            f"oracle+oracledb://{self.user}:{quote(self.password)}@{dsn}"
        )
        if self.oracle_home != "":
            os.environ["ORACLE_HOME"] = self.oracle_home
            return create_engine(
                connection_string,
                thick_mode={
                    "lib_dir": f"{self.oracle_home}/lib",
                    "config_dir": self.wallet_config,
                },
            )
        else:
            return create_engine(connection_string)

    async def get_cursor(self, query):
        """Executes the passed query on the Non-Async supported Database server and return cursor.

        Args:
            query (str): Database query to be executed.

        Returns:
            cursor: Synchronous cursor
        """
        self._logger.debug(f"Retrieving the cursor for query '{query}'")
        try:
            loop = asyncio.get_running_loop()
            if self.connection is None:
                self.connection = await loop.run_in_executor(
                    executor=None,
                    func=self.engine.connect,  # pyright: ignore
                )
            cursor = await loop.run_in_executor(
                executor=None,
                func=partial(self.connection.execute, statement=text(query)),
            )
            return cursor
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while getting cursor; error: {exception}"
            )
            raise

    async def ping(self):
        return await anext(
            fetch(
                cursor_func=partial(self.get_cursor, self.queries.ping()),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )

    async def get_tables_to_fetch(self):
        tables = configured_tables(self.tables)
        if is_wildcard(tables):
            self._logger.info(
                "Fetching all tables as the configuration field 'tables' is set to '*'"
            )
            async for row in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.all_tables(
                        user=self.user,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            ):
                yield row[0]
        else:
            self._logger.info(f"Fetching user-configured tables '{tables}'")
            for table in tables:
                yield table

    async def get_table_row_count(self, table):
        [row_count] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_data_count(
                        table=table,
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return row_count

    async def get_table_primary_key(self, table):
        if (self.primary_key_column is not None):
            self._logger.info(f"Primary key override found: {self.primary_key_column}")
            return [self.primary_key_column]
        
        self._logger.debug(f"Extracting primary keys for table '{table}'")
        primary_keys = [
            key
            async for [key] in fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_primary_key(
                        user=self.user,
                        table=table,
                    ),
                ),
                fetch_size=self.fetch_size,
                retry_count=self.retry_count,
            )
        ]
        self._logger.debug(f"Found primary keys for table '{table}'")
        return primary_keys

    async def get_table_last_update_time(self, table):
        self._logger.debug(f"Fetching last updated time for table '{table}'")
        [last_update_time] = await anext(
            fetch(
                cursor_func=partial(
                    self.get_cursor,
                    self.queries.table_last_update_time(
                        table=table,
                        updated_date_column=self.updated_date_column
                    ),
                ),
                fetch_size=1,
                retry_count=self.retry_count,
            )
        )
        return last_update_time

    async def data_streamer(self, table, timestamp=None):
        """Streaming data from a table

        Args:
            table (str): Table.

        Raises:
            exception: Raise an exception after retrieving

        Yields:
            list: It will first yield the column names, then data in each row
        """
        self._logger.debug(f"Streaming records from database for table '{table}'")
        record_count = 0
        async for data in fetch(
            cursor_func=partial(
                self.get_cursor,
                self.queries.table_data(
                    table=table,
                    timestamp=timestamp,
                    updated_date_column=self.updated_date_column
                ),
            ),
            fetch_columns=True,
            fetch_size=self.fetch_size,
            retry_count=self.retry_count,
        ):
            record_count += 1
            yield data
        self._logger.info(f"Found {record_count} records for table '{table}'")
    
    def get_updated_date_column(self):
        return self.updated_date_column
    
    def get_file_location_column(self):
        return self.file_location_column


class OracleDataSource(BaseDataSource):
    """Oracle Database"""

    name = "Oracle Database"
    service_type = "oracle"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup connection to the Oracle database-server configured by user

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self._http_session = None
        self._netrc_auth = None
        self._netrc_loaded = False
        self.database = (
            self.configuration["sid"]
            if self.configuration["connection_source"] == SID
            else self.configuration["service_name"]
        )
        self.oracle_client = OracleClient(
            host=self.configuration["host"],
            port=self.configuration["port"],
            user=self.configuration["username"],
            password=self.configuration["password"],
            connection_source=self.configuration["connection_source"],
            sid=self.configuration["sid"],
            service_name=self.configuration["service_name"],
            tables=self.configuration["tables"],
            updated_date_column=self.configuration["updated_date_column"],
            primary_key_column=self.configuration["primary_key_column"],
            protocol=self.configuration["oracle_protocol"],
            oracle_home=self.configuration["oracle_home"],
            wallet_config=self.configuration["wallet_configuration_path"],
            retry_count=self.configuration["retry_count"],
            fetch_size=self.configuration["fetch_size"],
            file_location_column=self.configuration["file_location_column"],
            logger_=self._logger,
        )

    def _set_internal_logger(self):
        self.oracle_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "host": {
                "label": "Host",
                "order": 1,
                "type": "str",
            },
            "port": {
                "display": "numeric",
                "label": "Port",
                "order": 2,
                "type": "int",
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "connection_source": {
                "display": "dropdown",
                "label": "Connection Source",
                "options": [
                    {"label": "SID", "value": SID},
                    {"label": "Service Name", "value": SERVICE_NAME},
                ],
                "order": 5,
                "type": "str",
                "value": SID,
                "tooltip": "Select 'Service Name' option if connecting to a pluggable database",
            },
            "sid": {
                "depends_on": [{"field": "connection_source", "value": SID}],
                "label": "SID",
                "order": 6,
                "type": "str",
            },
            "service_name": {
                "depends_on": [{"field": "connection_source", "value": SERVICE_NAME}],
                "label": "Service Name",
                "order": 7,
                "type": "str",
            },
            "tables": {
                "display": "textarea",
                "label": "Comma-separated list of tables",
                "options": [],
                "order": 8,
                "type": "list",
                "value": "*",
            },
            "updated_date_column": {
                "default_value": "LAST_UPDATE_DATE",
                "value": "LAST_UPDATE_DATE",
                "label": "The column name in the database which stores the date the row was last updated",
                "order": 9,
                "required": True,
                "type": "str",
            },
            "primary_key_column": {
                "default_value": "ASSIGNED_ID",
                "value": "ASSIGNED_ID",
                "label": "Column of the primary key of the table",
                "order": 10,
                "required": False,
                "type": "str",
            },
            "fetch_size": {
                "default_value": DEFAULT_FETCH_SIZE,
                "display": "numeric",
                "label": "Rows fetched per request",
                "order": 11,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "retry_count": {
                "default_value": DEFAULT_RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 12,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "oracle_protocol": {
                "default_value": DEFAULT_PROTOCOL,
                "display": "dropdown",
                "label": "Oracle connection protocol",
                "options": [
                    {"label": "TCP", "value": "TCP"},
                    {"label": "TCPS", "value": "TCPS"},
                ],
                "order": 13,
                "type": "str",
                "value": DEFAULT_PROTOCOL,
                "ui_restrictions": ["advanced"],
            },
            "oracle_home": {
                "default_value": DEFAULT_ORACLE_HOME,
                "label": "Path to Oracle Home",
                "order": 14,
                "required": False,
                "type": "str",
                "value": DEFAULT_ORACLE_HOME,
                "ui_restrictions": ["advanced"],
            },
            "wallet_configuration_path": {
                "default_value": "",
                "label": "Path to SSL Wallet configuration files",
                "order": 15,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "file_reference_column": {
                "default_value": "",
                "label": "Column containing CSV file IDs",
                "order": 16,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "file_download_url_template": {
                "default_value": "",
                "label": "File download URL template",
                "order": 17,
                "required": False,
                "type": "str",
                "tooltip": "Use {file_id} as a placeholder for the file ID from each row.",
                "ui_restrictions": ["advanced"],
            },
            "file_download_auth_header_name": {
                "default_value": "",
                "label": "File download auth header name",
                "order": 18,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "file_download_auth_header_value": {
                "default_value": "",
                "label": "File download auth header value",
                "order": 19,
                "required": False,
                "sensitive": True,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "netrc_path": {
                "default_value": "",
                "label": "Path to .netrc file",
                "order": 20,
                "required": False,
                "type": "str",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 21,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    async def handle_file_content_extraction(self, doc, source_filename, temp_filename):
        """
        Determines if file content should be extracted locally,
        or converted to b64 for pipeline extraction.

        Returns the `doc` arg with a new field:
            - `body` if local content extraction was used
            - `_attachment` if pipeline extraction will be used
        """
        if self.configuration.get("use_text_extraction_service"):
            if self.extraction_service._check_configured():
                doc["body"] = await self.extraction_service.extract_text(
                    temp_filename, source_filename
                )
                return

                # This is for multiple files, should work but not using text extraction service right now
                if "body" in doc and doc["body"] is not None:
                    doc["body"].append(
                        await self.extraction_service.extract_text(
                            temp_filename, source_filename
                        )
                    )
                else:
                    doc["body"] = [
                        await self.extraction_service.extract_text(
                            temp_filename, source_filename
                        )
                    ]
        else:
            self._logger.debug(f"Calling convert_to_b64 for file : {source_filename}")
            await asyncio.to_thread(convert_to_b64, source=temp_filename)
            async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
                doc["_attachment"] = (await async_buffer.read()).strip()
                return
            
                # The _attachment field cannot be an array
                if ("_attachment" in doc and doc["_attachment"] is not None):
                    # base64 on macOS will add a EOL, so we strip() here
                    doc["_attachment"].append((await async_buffer.read()).strip())
                else:
                    doc["_attachment"] = [(await async_buffer.read()).strip()]

        return doc

    def _file_reference_key(self, table):
        file_reference_column = self.configuration["file_reference_column"]
        if file_reference_column in (None, ""):
            return None
        return f"{table}_{file_reference_column}".lower()

    def _normalize_file_reference_ids(self, file_references):
        file_ids = []

        def _append_id(file_id):
            if isinstance(file_id, bool):
                self._logger.warning("Skipping boolean file ID value.")
                return
            if not isinstance(file_id, str):
                file_id = str(file_id)
            file_id = file_id.strip()
            if file_id == "":
                self._logger.warning("Skipping empty file ID value.")
                return
            file_ids.append(file_id)

        def _parse_references(references):
            if references is None:
                return
            if isinstance(references, str):
                stripped = references.strip()
                if stripped == "":
                    return
                if stripped.startswith(("{", "[")):
                    try:
                        parsed = json.loads(stripped)
                    except json.JSONDecodeError as exception:
                        self._logger.warning(
                            f"Malformed file ID JSON payload. Skipping value. Error: {exception}"
                        )
                        return
                    _parse_references(parsed)
                    return

                try:
                    parsed_ids = next(csv.reader([stripped], skipinitialspace=True))
                except csv.Error as exception:
                    self._logger.warning(
                        f"Malformed CSV file ID payload. Skipping value. Error: {exception}"
                    )
                    return

                for parsed_id in parsed_ids:
                    _append_id(parsed_id)
                return

            if isinstance(references, (list, tuple)):
                for value in references:
                    _parse_references(value)
                return

            if isinstance(references, dict):
                file_id = references.get("file_id")
                if file_id is None:
                    file_id = references.get("id")
                if file_id is None:
                    self._logger.warning(
                        "Skipping object file reference without `file_id` key."
                    )
                    return
                _parse_references(file_id)
                return

            _append_id(references)

        _parse_references(file_references)
        return file_ids

    def _build_file_url(self, file_id):
        template = self.configuration["file_download_url_template"]
        if template in (None, ""):
            self._logger.warning(
                "File download URL template is not configured. Skipping file download."
            )
            return None

        encoded_file_id = quote(str(file_id).strip(), safe="")
        if encoded_file_id == "":
            self._logger.warning("Skipping empty encoded file ID.")
            return None

        try:
            if "{file_id}" in template or "{id}" in template:
                file_url = template.format(file_id=encoded_file_id, id=encoded_file_id)
            else:
                separator = "" if template.endswith("/") else "/"
                file_url = f"{template}{separator}{encoded_file_id}"
        except (KeyError, IndexError, ValueError) as exception:
            self._logger.warning(
                f"Could not build file URL from template '{template}'. Error: {exception}"
            )
            return None

        parsed = urlparse(file_url)
        if parsed.scheme not in ("http", "https"):
            self._logger.warning(
                f"Skipping built file URL '{file_url}' due to unsupported scheme '{parsed.scheme}'."
            )
            return None

        return file_url

    async def _get_http_session(self):
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    def _file_download_headers(self):
        header_name = self.configuration["file_download_auth_header_name"]
        header_value = self.configuration["file_download_auth_header_value"]
        if header_name and header_value:
            return {header_name: header_value}
        return None

    def _load_netrc_auth(self):
        if self._netrc_loaded:
            return

        netrc_path = self.configuration["netrc_path"]
        if netrc_path in (None, ""):
            self._netrc_loaded = True
            return

        try:
            self._netrc_auth = netrc.netrc(netrc_path)
        except (FileNotFoundError, netrc.NetrcParseError, PermissionError) as exception:
            self._logger.warning(
                f"Unable to load .netrc file at '{netrc_path}'. Error: {exception}"
            )
        finally:
            self._netrc_loaded = True

    def _get_netrc_auth(self, url):
        self._load_netrc_auth()
        if self._netrc_auth is None:
            return None

        host = urlparse(url).hostname
        if host is None:
            return None

        credentials = self._netrc_auth.authenticators(host)
        if credentials is None:
            return None

        login, _, password = credentials
        if not login or not password:
            return None

        return aiohttp.BasicAuth(login=login, password=password)

    async def _http_chunked_download_func(self, url, source_filename):
        session = await self._get_http_session()
        headers = self._file_download_headers()
        auth = self._get_netrc_auth(url)
        async with session.get(url=url, headers=headers, auth=auth) as response:
            if not response.ok:
                self._logger.warning(
                    f"Failed to download '{source_filename}' from '{url}'. HTTP status: {response.status}"
                )
                raise Exception(f"Failed downloading file with status {response.status}")

            file_size = response.content_length
            if file_size is not None and not self.is_file_size_within_limit(
                file_size, source_filename
            ):
                self._logger.warning(
                    f"Skipping '{source_filename}' because it exceeds size limits."
                )
                raise Exception("File exceeds max file size")

            async for data in response.content.iter_chunked(
                DEFAULT_HTTP_DOWNLOAD_CHUNK_SIZE
            ):
                yield data

    async def get_content(self, doc, file_urls, timestamp=None, doit=False):
        if not doit:
            return

        if not file_urls:
            self._logger.warning(f"No file urls found for doc {doc['id']}")
            return

        extracted_content = { }

        any_download_attempted = False
        for file_url in file_urls:
            parsed = urlparse(file_url)
            source_filename = unquote(parsed.path.rsplit("/", maxsplit=1)[-1])
            if source_filename == "":
                source_filename = "downloaded_file"

            file_extension = self.get_file_extension(source_filename)
            if file_extension and not self.is_valid_file_type(
                file_extension, source_filename
            ):
                self._logger.warning(
                    f"Skipping file URL '{file_url}' because extension '{file_extension}' is not supported."
                )
                continue

            any_download_attempted = True
            extracted_content = await self.download_and_extract_file(
                extracted_content,
                source_filename,
                file_extension,
                partial(self._http_chunked_download_func, file_url, source_filename),
                return_doc_if_failed=True,
            )

        if any_download_attempted:
            return extracted_content

    async def close(self):
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
        self.oracle_client.close()

    async def ping(self):
        """Verify the connection with the database-server configured by user"""
        self._logger.debug("Validating that the Connector can connect to Oracle...")
        try:
            await self.oracle_client.ping()
            self._logger.debug("Successfully connected to Oracle")
        except Exception as e:
            msg = f"Can't connect to Oracle on {self.oracle_client.host}"
            raise Exception(msg) from e

    async def fetch_documents(self, table, timestamp=None):
        """Fetches all the table entries and format them in Elasticsearch documents

        Args:
            table (str): Name of table

        Yields:
            Dict: Document to be indexed
        """
        try:
            self._logger.info(f"Fetching records for table '{table}'")
            row_count = await self.oracle_client.get_table_row_count(table=table)
            if row_count > 0:
                # Query to get the table's primary key
                self._logger.debug(f"Total {row_count} rows found in table '{table}'")
                keys = await self.oracle_client.get_table_primary_key(table=table)
                keys = map_column_names(column_names=keys, tables=[table])
                if keys:
                    try:
                        last_update_time = (
                            await self.oracle_client.get_table_last_update_time(
                                table=table
                            )
                        )
                        last_update_time = last_update_time.strftime('%Y-%m-%d %H:%M:%S')
                        self._logger.info(f"Most recent update date in the table is {last_update_time}")
                    except Exception as e:
                        self._logger.warning(
                            f"Unable to fetch last updated time for table '{table}'; error: {e}"
                        )
                        last_update_time = None
                    streamer = self.oracle_client.data_streamer(table=table, timestamp=timestamp)
                    column_names = await anext(streamer)
                    column_names = map_column_names(
                        column_names=column_names, tables=[table]
                    )
                    async for row in streamer:
                        row = dict(zip(column_names, row, strict=True))

                        self._logger.debug(row)

                        row_time = row.get(f"{table.lower()}_{self.oracle_client.get_updated_date_column().lower()}")
                        #self._logger.info(f"Row time: {row_time}")
                        doc_update_time = iso_utc(row_time)
                        keys_value = ""
                        for key in keys:
                            keys_value += f"{row.get(key)}_" if row.get(key) else ""

                        row.update(
                            {
                                "_id": f"{self.database}_{table}_{keys_value}",
                                "_timestamp": doc_update_time or iso_utc(),
                                "Database": self.database,
                                "Table": table,
                            }
                        )

                        serialized = self.serialize(doc=row)

                        file_reference_key = self._file_reference_key(table=table)
                        if file_reference_key is not None:
                            if file_reference_key not in serialized:
                                self._logger.warning(
                                    f"Configured file reference column '{self.configuration['file_reference_column']}' is missing in table '{table}' row."
                                )
                            else:
                                normalized_file_ids = self._normalize_file_reference_ids(
                                    serialized.get(file_reference_key)
                                )
                                file_urls = []
                                for file_id in normalized_file_ids:
                                    file_url = self._build_file_url(file_id)
                                    if file_url:
                                        file_urls.append(file_url)

                                if file_urls:
                                    serialized[ORACLE_FILE_URLS_FIELD] = file_urls

                        yield serialized

                    self.update_sync_timestamp_cursor(last_update_time)
                else:
                    self._logger.warning(
                        f"Skipping '{table}' table from database '{self.database}' since no primary key is associated with it. Assign a primary key to the table to index it in the next sync interval."
                    )
            else:
                self._logger.warning(f"No records found for table '{table}'")
        except (InternalClientError, ProgrammingError) as exception:
            self._logger.warning(
                f"Something went wrong while fetching records from table '{table}'; error: {exception}"
            )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch databases, tables and rows in async manner.

        Yields:
            dictionary: Row dictionary containing meta-data of the row.
        """
        table_count = 0
        async for table in self.oracle_client.get_tables_to_fetch():
            table_count += 1
            async for row in self.fetch_documents(table=table):
                file_urls = row.pop(ORACLE_FILE_URLS_FIELD, [])
                lazy_download = None
                if file_urls:
                    lazy_download = (partial(self.get_content, doc=row, file_urls=file_urls) 
                                if (row["search.elastic_inl_documents_vw_restricted_flag"] == False 
                                    or row["search.elastic_inl_documents_vw_restricted_flag"] == "N"
                                    or row["search.elastic_inl_documents_vw_restricted_flag"] == "false") 
                                else None)
                yield row, lazy_download
        if table_count < 1:
            self._logger.warning(f"Fetched 0 tables for the database '{self.database}'")

    async def get_docs_incrementally(self, sync_cursor, filtering=None):
        self._sync_cursor = sync_cursor
        timestamp = self.last_sync_time()
        timestamp = parse_datetime_string(timestamp).strftime('%Y-%m-%d %H:%M:%S')

        self._logger.info(f"Sync cursor time is: {timestamp}")

        table_count = 0
        async for table in self.oracle_client.get_tables_to_fetch():
            table_count += 1
            async for row in self.fetch_documents(table=table, timestamp=timestamp):
                file_urls = row.pop(ORACLE_FILE_URLS_FIELD, [])
                lazy_download = None
                if file_urls and (row["search.elastic_inl_documents_vw_restricted_flag"] == False 
                                    or row["search.elastic_inl_documents_vw_restricted_flag"] == "N"
                                    or row["search.elastic_inl_documents_vw_restricted_flag"] == "false"):
                    lazy_download = partial(self.get_content, doc=row, file_urls=file_urls)
                yield row, lazy_download, OP_INDEX

        if table_count < 1:
            self._logger.warning(f"Fetched 0 tables for the database '{self.database}'")
