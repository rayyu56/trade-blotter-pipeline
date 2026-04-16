"""Tests for agent/tools/file_uploader.py."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agent.tools.file_uploader import upload_file_to_workspace, UploadResult

HOST = "https://dbc-test.cloud.databricks.com"
TOKEN = "dapi-test-token"
WORKSPACE_DIR = "/Users/ray@example.com/project/data/bronze"


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """A small CSV file on the local filesystem."""
    f = tmp_path / "trades_20260401.csv"
    f.write_text("trade_id,symbol\nTRD-001,AAPL\nTRD-002,MSFT\n")
    return f


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestUploadFileToWorkspace:
    def test_returns_upload_result(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            result = upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        assert isinstance(result, UploadResult)

    def test_workspace_path_preserves_filename(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response):
            result = upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        assert result.workspace_path == f"{WORKSPACE_DIR}/{csv_file.name}"

    def test_size_bytes_matches_file(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response):
            result = upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        assert result.size_bytes == csv_file.stat().st_size

    def test_calls_correct_api_endpoint(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        url = mock_post.call_args[0][0]
        assert url == f"{HOST}/api/2.0/workspace/import"

    def test_sends_bearer_token(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        headers = mock_post.call_args[1]["headers"]
        assert headers["Authorization"] == f"Bearer {TOKEN}"

    def test_payload_contains_base64_content(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        payload = mock_post.call_args[1]["json"]
        decoded = base64.b64decode(payload["content"])
        assert decoded == csv_file.read_bytes()

    def test_payload_sets_format_auto(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        payload = mock_post.call_args[1]["json"]
        assert payload["format"] == "AUTO"

    def test_overwrite_true_by_default(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)

        payload = mock_post.call_args[1]["json"]
        assert payload["overwrite"] is True

    def test_overwrite_false_is_forwarded(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response) as mock_post:
            upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR, overwrite=False)

        payload = mock_post.call_args[1]["json"]
        assert payload["overwrite"] is False

    def test_trailing_slash_stripped_from_workspace_dir(self, csv_file: Path):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response):
            result = upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR + "/")

        assert "//" not in result.workspace_path


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestUploadErrors:
    def test_missing_local_file_raises_file_not_found(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            upload_file_to_workspace(HOST, TOKEN, str(tmp_path / "nonexistent.csv"), WORKSPACE_DIR)

    def test_http_error_is_propagated(self, csv_file: Path):
        import requests as req
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = req.HTTPError("403 Forbidden")

        with patch("agent.tools.file_uploader.requests.post", return_value=mock_response):
            with pytest.raises(req.HTTPError):
                upload_file_to_workspace(HOST, TOKEN, str(csv_file), WORKSPACE_DIR)
