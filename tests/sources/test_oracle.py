#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Oracle Database source class methods"""

import os
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.engine import Engine

from connectors.sources.oracle import (
    ORACLE_FILE_URLS_FIELD,
    OracleClient,
    OracleDataSource,
    OracleQueries,
)
from tests.sources.support import create_source
from tests.sources.test_generic_database import ConnectionSync

DSN_SID = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(SID=xe)))"
DSN_SERVICE_NAME = "oracle+oracledb://admin:Password_123@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=9090))(CONNECT_DATA=(service_name=xe)))"
SID = "sid"
SERVICE_NAME = "service_name"


@contextmanager
def oracle_client(**extras):
    arguments = {
        "host": "127.0.0.1",
        "port": 9090,
        "user": "admin",
        "password": "Password_123",
        "connection_source": SID,
        "sid": "xe",
        "service_name": "xe",
        "tables": "*",
        "protocol": "TCP",
        "oracle_home": "",
        "wallet_config": "",
        "logger_": None,
    } | extras

    client = OracleClient(**arguments)
    try:
        yield client
    finally:
        client.close()


@patch("connectors.sources.oracle.create_engine")
@pytest.mark.parametrize(
    "connection_source, DSN",
    [
        (SID, DSN_SID),
        (SERVICE_NAME, DSN_SERVICE_NAME),
    ],
)
def test_engine_in_thin_mode(mock_fun, connection_source, DSN):
    """Test engine method of OracleClient class in thin mode"""
    # Setup
    with oracle_client() as client:
        # Execute
        client.connection_source = connection_source
        _ = client.engine

        # Assert
        mock_fun.assert_called_with(DSN)


@patch("connectors.sources.oracle.create_engine")
@pytest.mark.parametrize(
    "connection_source, DSN",
    [
        (SID, DSN_SID),
        (SERVICE_NAME, DSN_SERVICE_NAME),
    ],
)
def test_engine_in_thick_mode(mock_fun, connection_source, DSN):
    """Test engine method of OracleClient class in thick mode"""
    oracle_home = "/home/devuser"
    config_file_path = {"lib_dir": f"{oracle_home}/lib", "config_dir": ""}

    # Setup
    with oracle_client(oracle_home="/home/devuser") as client:
        client.connection_source = connection_source
        mock_fun.return_value = "Mock Response"

        # Execute
        _ = client.engine

        # Assert
        mock_fun.assert_called_with(DSN, thick_mode=config_file_path)


@pytest.mark.asyncio
async def test_ping():
    async with create_source(OracleDataSource) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(OracleQueries())
        ):
            await source.ping()


@pytest.mark.asyncio
async def test_get_docs():
    # Setup
    async with create_source(
        OracleDataSource,
        username="admin",
        password="changeme",
        data_source=SID,
        sid="xe",
        tables="*",
    ) as source:
        with patch.object(
            Engine, "connect", return_value=ConnectionSync(OracleQueries())
        ):
            actual_response = []
            expected_response = [
                {
                    "emp_table_ids": 1,
                    "emp_table_names": "abcd",
                    "_id": "xe_emp_table_1_",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "Database": "xe",
                    "Table": "emp_table",
                },
                {
                    "emp_table_ids": 2,
                    "emp_table_names": "xyz",
                    "_id": "xe_emp_table_2_",
                    "_timestamp": "2023-02-21T08:37:15+00:00",
                    "Database": "xe",
                    "Table": "emp_table",
                },
            ]

            # Execute
            async for doc in source.get_docs():
                assert doc[1] is None
                actual_response.append(doc[0])

            # Assert
            assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_docs_with_file_references_returns_lazy_download():
    async with create_source(
        OracleDataSource,
        file_reference_column="file_urls",
    ) as source:

        async def _mock_fetch_documents(table, timestamp=None):
            yield {
                "_id": "xe_emp_table_1_",
                "_timestamp": "2023-02-21T08:37:15+00:00",
                "emp_table_ids": 1,
                ORACLE_FILE_URLS_FIELD: ["https://example.com/doc.txt"],
            }

        source.fetch_documents = _mock_fetch_documents

        async def _mock_get_tables_to_fetch():
            yield "emp_table"

        source.oracle_client.get_tables_to_fetch = _mock_get_tables_to_fetch

        docs = [doc async for doc in source.get_docs()]
        assert len(docs) == 1
        row, lazy_download = docs[0]
        assert ORACLE_FILE_URLS_FIELD not in row
        assert lazy_download is not None


@pytest.mark.parametrize(
    "raw_reference, expected_ids",
    [
        ("123", ["123"]),
        ("123,456,789", ["123", "456", "789"]),
        ("123, 456 , 789", ["123", "456", "789"]),
        ('"123,456",789', ["123,456", "789"]),
        (
            ["123", "456"],
            ["123", "456"],
        ),
        (
            '[{"file_id":"123"},{"file_id":"456"}]',
            ["123", "456"],
        ),
        (
            '{"file_id":"123"}',
            ["123"],
        ),
        (
            '{"id":"123"}',
            ["123"],
        ),
        ("[not-json", []),
        ({"invalid": "value"}, []),
        (123, ["123"]),
    ],
)
@pytest.mark.asyncio
async def test_normalize_file_reference_ids(raw_reference, expected_ids):
    async with create_source(OracleDataSource) as source:
        normalized = source._normalize_file_reference_ids(raw_reference)
        assert normalized == expected_ids


@pytest.mark.asyncio
async def test_build_file_url_from_template():
    async with create_source(
        OracleDataSource,
        file_download_url_template="https://files.example.com/download/{file_id}",
    ) as source:
        assert (
            source._build_file_url("abc/123")
            == "https://files.example.com/download/abc%2F123"
        )


@pytest.mark.asyncio
async def test_build_file_url_when_template_has_no_placeholder():
    async with create_source(
        OracleDataSource,
        file_download_url_template="https://files.example.com/download",
    ) as source:
        assert (
            source._build_file_url("123")
            == "https://files.example.com/download/123"
        )


@pytest.mark.asyncio
async def test_get_content_merges_multiple_downloaded_files():
    async with create_source(OracleDataSource) as source:
        source.is_valid_file_type = MagicMock(return_value=True)

        async def _mock_download_and_extract_file(
            doc,
            source_filename,
            file_extension,
            download_func,
            return_doc_if_failed=False,
        ):
            attachment = doc.get("_attachment", [])
            attachment.append(source_filename)
            doc["_attachment"] = attachment
            return doc

        source.download_and_extract_file = _mock_download_and_extract_file

        content = await source.get_content(
            doc={"_id": "doc-1", "_timestamp": "2024-01-01T00:00:00+00:00"},
            file_urls=[
                "https://example.com/a.txt",
                "https://example.com/b.txt",
            ],
            doit=True,
        )
        assert content["_attachment"] == ["a.txt", "b.txt"]


@pytest.mark.asyncio
async def test_get_content_continues_after_one_file_failure():
    async with create_source(OracleDataSource) as source:
        source.is_valid_file_type = MagicMock(return_value=True)

        async def _mock_download_and_extract_file(
            doc,
            source_filename,
            file_extension,
            download_func,
            return_doc_if_failed=False,
        ):
            if source_filename == "a.txt":
                return doc
            attachment = doc.get("_attachment", [])
            attachment.append(source_filename)
            doc["_attachment"] = attachment
            return doc

        source.download_and_extract_file = _mock_download_and_extract_file

        content = await source.get_content(
            doc={"_id": "doc-1", "_timestamp": "2024-01-01T00:00:00+00:00"},
            file_urls=[
                "https://example.com/a.txt",
                "https://example.com/b.txt",
            ],
            doit=True,
        )
        assert content["_attachment"] == ["b.txt"]


@pytest.mark.asyncio
async def test_get_content_preserves_metadata_when_download_fails():
    async with create_source(OracleDataSource) as source:
        source.is_valid_file_type = MagicMock(return_value=True)

        async def _mock_download_and_extract_file(
            doc,
            source_filename,
            file_extension,
            download_func,
            return_doc_if_failed=False,
        ):
            return doc if return_doc_if_failed else None

        source.download_and_extract_file = _mock_download_and_extract_file

        original_doc = {
            "_id": "doc-1",
            "_timestamp": "2024-01-01T00:00:00+00:00",
            "emp_table_epower_files": "123",
            "emp_table_title": "Important metadata",
        }
        content = await source.get_content(
            doc=original_doc,
            file_urls=["https://example.com/a.txt"],
            doit=True,
        )

        assert content["_id"] == original_doc["_id"]
        assert content["_timestamp"] == original_doc["_timestamp"]
        assert (
            content["emp_table_epower_files"]
            == original_doc["emp_table_epower_files"]
        )
        assert content["emp_table_title"] == original_doc["emp_table_title"]


@pytest.mark.asyncio
async def test_file_download_headers():
    async with create_source(
        OracleDataSource,
        file_download_auth_header_name="Authorization",
        file_download_auth_header_value="Bearer token",
    ) as source:
        assert source._file_download_headers() == {"Authorization": "Bearer token"}


@pytest.mark.asyncio
async def test_http_chunked_download_func_uses_configured_headers():
    async with create_source(
        OracleDataSource,
        file_download_auth_header_name="Authorization",
        file_download_auth_header_value="Bearer token",
    ) as source:
        session = MagicMock()
        session.closed = False
        response = MagicMock()
        response.ok = True
        response.content_length = 128

        async def _iter_chunked(_chunk_size):
            yield b"chunk"

        response.content.iter_chunked = _iter_chunked
        context_manager = MagicMock()
        context_manager.__aenter__ = AsyncMock(return_value=response)
        context_manager.__aexit__ = AsyncMock(return_value=None)
        session.get = MagicMock(return_value=context_manager)
        source._http_session = session
        source.is_file_size_within_limit = MagicMock(return_value=True)

        data = [
            chunk
            async for chunk in source._http_chunked_download_func(
                url="https://example.com/a.txt", source_filename="a.txt"
            )
        ]
        assert data == [b"chunk"]
        session.get.assert_called_once_with(
            url="https://example.com/a.txt",
            headers={"Authorization": "Bearer token"},
            auth=None,
        )


@pytest.mark.asyncio
async def test_get_netrc_auth_from_configured_file(tmp_path):
    netrc_path = tmp_path / "test.netrc"
    netrc_path.write_text(
        "machine files.example.com login connector password secret\n",
        encoding="utf-8",
    )
    os.chmod(netrc_path, 0o600)

    async with create_source(
        OracleDataSource,
        netrc_path=str(netrc_path),
    ) as source:
        auth = source._get_netrc_auth("https://files.example.com/download/123")
        assert auth is not None
        assert auth.login == "connector"
        assert auth.password == "secret"


@pytest.mark.asyncio
async def test_http_chunked_download_func_uses_netrc_auth(tmp_path):
    netrc_path = tmp_path / "test.netrc"
    netrc_path.write_text(
        "machine files.example.com login connector password secret\n",
        encoding="utf-8",
    )
    os.chmod(netrc_path, 0o600)

    async with create_source(
        OracleDataSource,
        netrc_path=str(netrc_path),
    ) as source:
        session = MagicMock()
        session.closed = False
        response = MagicMock()
        response.ok = True
        response.content_length = 128

        async def _iter_chunked(_chunk_size):
            yield b"chunk"

        response.content.iter_chunked = _iter_chunked
        context_manager = MagicMock()
        context_manager.__aenter__ = AsyncMock(return_value=response)
        context_manager.__aexit__ = AsyncMock(return_value=None)
        session.get = MagicMock(return_value=context_manager)
        source._http_session = session
        source.is_file_size_within_limit = MagicMock(return_value=True)

        _ = [
            chunk
            async for chunk in source._http_chunked_download_func(
                url="https://files.example.com/a.txt", source_filename="a.txt"
            )
        ]

        call_kwargs = session.get.call_args.kwargs
        assert call_kwargs["auth"] is not None
        assert call_kwargs["auth"].login == "connector"
        assert call_kwargs["auth"].password == "secret"


@pytest.mark.asyncio
async def test_close_closes_http_session():
    async with create_source(OracleDataSource) as source:
        source._http_session = AsyncMock()
        source._http_session.closed = False
        source.oracle_client.close = MagicMock()

        await source.close()

        source._http_session.close.assert_awaited_once()
        source.oracle_client.close.assert_called_once()
