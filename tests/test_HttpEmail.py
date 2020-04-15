import json

import pytest

from FunctionAutomate.HttpEmail import get_param, parse_request

import azure.functions as func


@pytest.fixture()
def request_1():
    req = func.HttpRequest(
        method="GET",
        body=json.dumps({"foo": "bar", "name": "body"}).encode(),
        url="/api/x",
        params={"name": "Test"},
    )
    yield req


@pytest.fixture()
def request_2():
    req = func.HttpRequest(
        method="GET",
        body=b"",
        url="/api/x",
        params={
            "user": "user",
            "subject": "Hello",
            "recipients": "a@a.com,b@b.com",
            "body": "body",
            "mimetype": "plain",
        },
    )
    yield req


@pytest.fixture()
def request_3():
    req = func.HttpRequest(
        method="GET",
        body=b"",
        url="/api/x",
        params={"user": "user", "recipients": "a@a.com,b@b.com",},
    )
    yield req


class TestGetParams:
    def test_get_simple_param(self, request_1):
        assert get_param(request_1, "name") == "Test"

    def test_get_param_from_body(self, request_1):
        assert get_param(request_1, "foo") == "bar"

    def test_fail_get_param(self, request_1):
        assert get_param(request_1, "foobar") is None

    def test_json_takes_priority(self, request_1):
        assert get_param(request_1, "name") == "Test"


class TestParseRequest:
    def test_good_request(self, request_2):
        email_params = parse_request(request_2)
        expected = {
            "user": "user",
            "subject": "Hello",
            "recipients": ["a@a.com", "b@b.com"],
            "body": "body",
            "mimetype": "plain",
        }
        assert all([email_params[k] == expected[k] for k in expected])

    def test_default_params(self, request_3):
        email_params = parse_request(request_3)
        expected = {
            "user": "user",
            "subject": "",
            "recipients": ["a@a.com", "b@b.com"],
            "body": "",
            "mimetype": "plain",
        }
        assert all([email_params[k] == expected[k] for k in expected])
