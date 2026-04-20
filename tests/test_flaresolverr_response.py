from __future__ import annotations

import time

from cyberdrop_dl.clients.flaresolverr import Solution, _parse_cookies
from cyberdrop_dl.clients.response import _FlareSolverrResponse, _infer_content_type_from_body

# ---------------------------------------------------------------------------
# Fixtures: example FlareSolverr JSON responses
# ---------------------------------------------------------------------------

FLARESOLVERR_RESPONSE_EMPTY_HEADERS = {
    "status": "ok",
    "message": "Challenge solved!",
    "solution": {
        "url": "https://1337x.to/cat/Movies/1/",
        "status": 200,
        "cookies": [
            {
                "domain": ".1337x.to",
                "expiry": 1808054295,
                "httpOnly": True,
                "name": "cf_clearance",
                "path": "/",
                "sameSite": "None",
                "secure": True,
                "value": "KKW9gSBPiS8pWkenAaGd82lMQZwcCqSEALdTvs13Tf7QIdxHRN4NKdwhnut21rKA",
            }
        ],
        "userAgent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "headers": {},
        "response": '<html><head>\n<meta charset="utf></html>',
    },
    "startTimestamp": 1776518283422,
    "endTimestamp": 1776518297487,
    "version": "3.4.6",
}


# ---------------------------------------------------------------------------
# _infer_content_type_from_body
# ---------------------------------------------------------------------------


def test_infer_content_type_html() -> None:
    assert _infer_content_type_from_body("<html>test</html>") == "text/html"


def test_infer_content_type_html_with_leading_whitespace() -> None:
    assert _infer_content_type_from_body("  \n <html>") == "text/html"


def test_infer_content_type_json_object() -> None:
    assert _infer_content_type_from_body('{"key": "value"}') == "application/json"


def test_infer_content_type_json_array() -> None:
    assert _infer_content_type_from_body("[1, 2, 3]") == "application/json"


def test_infer_content_type_empty_string() -> None:
    assert _infer_content_type_from_body("") == ""


def test_infer_content_type_only_whitespace() -> None:
    assert _infer_content_type_from_body("   \n\t  ") == ""


# ---------------------------------------------------------------------------
# _parse_cookies
# ---------------------------------------------------------------------------


def test_parse_cookies_complete_cookie() -> None:
    cookies = [
        {
            "domain": ".example.com",
            "name": "session",
            "path": "/",
            "value": "abc123",
            "secure": True,
            "expires": int(time.time()) + 3600,
        }
    ]
    result = _parse_cookies(cookies)
    assert "session" in result
    assert result["session"].value == "abc123"
    assert result["session"]["domain"] == ".example.com"
    assert result["session"]["secure"] == "TRUE"
    assert int(result["session"]["max-age"]) > 0


def test_parse_cookies_missing_secure_and_expires() -> None:
    """Cookie without 'secure' and 'expires' must not raise KeyError."""
    cookies = [
        {
            "domain": ".example.com",
            "name": "test",
            "path": "/",
            "value": "xyz",
        }
    ]
    result = _parse_cookies(cookies)
    assert "test" in result
    assert result["test"].value == "xyz"
    assert result["test"]["secure"] == ""
    assert result["test"]["max-age"] == ""


def test_parse_cookies_secure_false() -> None:
    cookies = [
        {
            "domain": ".example.com",
            "name": "nosec",
            "path": "/",
            "value": "val",
            "secure": False,
        }
    ]
    result = _parse_cookies(cookies)
    assert result["nosec"]["secure"] == ""


def test_parse_cookies_expired_cookie() -> None:
    """An already-expired cookie should have max-age = 0."""
    cookies = [
        {
            "domain": ".example.com",
            "name": "old",
            "path": "/",
            "value": "stale",
            "expires": 1000000000,
        }
    ]
    result = _parse_cookies(cookies)
    assert result["old"]["max-age"] == "0"


def test_parse_cookies_empty_list() -> None:
    result = _parse_cookies([])
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Solution.from_dict
# ---------------------------------------------------------------------------


def test_solution_from_dict_with_empty_headers() -> None:
    """Parsing the full FlareSolverr solution dict must not raise."""
    solution_data = FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"]
    solution = Solution.from_dict(solution_data)
    assert solution.status == 200
    assert str(solution.url) == "https://1337x.to/cat/Movies/1/"
    assert solution.content == '<html><head>\n<meta charset="utf></html>'
    assert len(solution.headers) == 0
    assert "cf_clearance" in solution.cookies


def test_solution_from_dict_cookies_parsed_correctly() -> None:
    solution_data = FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"]
    solution = Solution.from_dict(solution_data)
    morsel = solution.cookies["cf_clearance"]
    assert morsel.value == "KKW9gSBPiS8pWkenAaGd82lMQZwcCqSEALdTvs13Tf7QIdxHRN4NKdwhnut21rKA"
    assert morsel["domain"] == ".1337x.to"
    assert morsel["path"] == "/"
    assert morsel["secure"] == "TRUE"


# ---------------------------------------------------------------------------
# _FlareSolverrResponse.create (async)
# ---------------------------------------------------------------------------


async def test_flaresolverr_response_infers_html_from_empty_headers() -> None:
    """When FlareSolverr returns empty headers, content-type should be inferred from the body."""
    solution = Solution.from_dict(FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"])
    response = _FlareSolverrResponse.create(solution)
    assert response.content_type == "text/html"
    assert response.status == 200
    assert response.location is None


async def test_flaresolverr_response_reads_text() -> None:
    solution = Solution.from_dict(FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"])
    response = _FlareSolverrResponse.create(solution)
    text = await response.text()
    assert "<html>" in text


async def test_flaresolverr_response_with_explicit_content_type() -> None:
    """When headers contain Content-Type, it should be used instead of inference."""
    solution_data = {
        **FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"],
        "headers": {"Content-Type": "application/json"},
        "response": '{"data": true}',
    }
    solution = Solution.from_dict(solution_data)
    response = _FlareSolverrResponse.create(solution)
    assert response.content_type == "application/json"


async def test_flaresolverr_response_empty_body_and_empty_headers() -> None:
    """Empty body + empty headers should result in empty content-type string."""
    solution_data = {
        **FLARESOLVERR_RESPONSE_EMPTY_HEADERS["solution"],
        "response": "",
    }
    solution = Solution.from_dict(solution_data)
    response = _FlareSolverrResponse.create(solution)
    assert response.content_type == ""
