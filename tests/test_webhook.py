import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from cyberdrop_dl.models import AppriseURL
from cyberdrop_dl.utils.webhook import send_webhook_notification

webhook = AppriseURL.model_validate({"url": "https://example.com/webhook", "tags": {"no_logs"}})


@contextlib.contextmanager
def _mock_request(mock_response: AsyncMock):
    with patch("aiohttp.request") as mock_request:
        mock_request.return_value.__aenter__.return_value = mock_response
        yield


async def test_send_webhook_message_success(caplog: pytest.LogCaptureFixture) -> None:
    mock_response = AsyncMock()
    mock_response.ok = True
    mock_response.status = 200

    with _mock_request(mock_response):
        with caplog.at_level(10):
            await send_webhook_notification("test", webhook)

        assert "Webhook notifications: Success" in caplog.text


async def test_send_webhook_message_failure_with_json_error(caplog: pytest.LogCaptureFixture) -> None:
    mock_response = AsyncMock()
    mock_response.ok = False
    mock_response.status = 400
    mock_response.json = AsyncMock(return_value={"error": "Bad Request", "content": "details"})

    with _mock_request(mock_response):
        with caplog.at_level(10):
            await send_webhook_notification("test", webhook)

        assert "Webhook notification failed:" in caplog.text
        assert "Bad Request" in caplog.text


async def test_send_webhook_message_failure_with_non_json_error(caplog: pytest.LogCaptureFixture) -> None:
    mock_response = AsyncMock()
    mock_response.ok = False
    mock_response.status = 500
    mock_response.json = AsyncMock(side_effect=aiohttp.ClientError("JSON error"))

    mock_response.raise_for_status = MagicMock(
        side_effect=aiohttp.ClientResponseError(
            AsyncMock(spec=aiohttp.RequestInfo),
            (),
            status=500,
        )
    )

    with _mock_request(mock_response):
        with caplog.at_level(10):
            await send_webhook_notification("test", webhook)

        assert "ClientResponseError: 500" in caplog.text
