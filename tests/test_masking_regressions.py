"""접속 문자열과 잘린 로그에서도 비밀값 전체를 가리는지 검증한다."""
import json

import pytest

from core.masking import mask_dsn, mask_text


@pytest.mark.parametrize("source,expected", [
    ("host=db user=test password=example", "host=db user=test password=***"),
    ("password = 'example secret' host=db", "password = '***' host=db"),
    (r"password='example\' secret' host=db", "password='***' host=db"),
    (r"password=example\ secret host=db", "password=*** host=db"),
    ("postgresql://test:example%20secret@db/app?password=second", "postgresql://test:***@db/app?password=***"),
    ("postgresql://db/app?password=example&sslmode=require", "postgresql://db/app?password=***&sslmode=require"),
])
def test_dsn_hides_keyword_and_uri_credentials(source, expected):
    assert mask_dsn(source) == expected


@pytest.mark.parametrize("source,expected", [
    ('{"password":"example secret","keep":1}', '{"password":"***","keep":1}'),
    ('{"password":"example \\" secret","keep":1}', '{"password":"***","keep":1}'),
    ("token='example, secret' keep=1", "token='***' keep=1"),
    ('{"password":"example secret', '{"password":"***'),
    ("password='example secret", "password='***"),
    ('{"token":{"nested":"example secret"', '{"token":***'),
    ("postgresql://test:example", "postgresql://test:***"),
])
def test_text_preserves_format_and_masks_complete_or_truncated_values(source, expected):
    assert mask_text(source) == expected


def test_nested_json_secret_values_are_replaced_regardless_of_type():
    source = r'{"items":[{"pa\u0073sword":"example secret","token":["hidden"]}],"secret":{"nested":"hidden"},"keep":1}'
    assert json.loads(mask_text(source)) == {
        "items": [{"password": "***", "token": "***"}],
        "secret": "***", "keep": 1,
    }


@pytest.mark.parametrize("mask", [mask_dsn, mask_text])
def test_keyword_dsn_unquoted_punctuation_is_part_of_password(mask):
    assert mask("host=db password=example,secret&tail user=test") == "host=db password=*** user=test"


def test_json_numeric_and_boolean_secrets_remain_valid_json():
    assert json.loads(mask_text('{"password":123,"token":false,"keep":2}')) == {
        "password": "***", "token": "***", "keep": 2,
    }


def test_json_host_text_does_not_change_nested_secret_parsing():
    source = '{"message":" host=db ","secret":{"value": "example secret"},"keep":1}'
    assert json.loads(mask_text(source)) == {"message": " host=db ", "secret": "***", "keep": 1}


def test_dsn_uri_percent_encoded_password_key():
    assert mask_dsn("postgresql://db/app?%70assword=example%20secret") == "postgresql://db/app?%70assword=***"


def test_json_string_embedded_quoted_password_is_fully_masked():
    source = json.dumps({'message': 'password="example secret"'})
    assert json.loads(mask_text(source)) == {'message': 'password="***"'}


def test_json_string_embedded_keyword_dsn_is_fully_masked():
    source = json.dumps({'dsn': 'host=db password=example,tail user=test'})
    assert json.loads(mask_text(source)) == {'dsn': 'host=db password=*** user=test'}


def test_truncated_json_string_hides_embedded_secret_suffix():
    source = json.dumps({'message': 'password="example secret"'})[:-3]
    result = mask_text(source)
    assert 'example' not in result and 'secret' not in result


@pytest.mark.parametrize('value', ['password="example secret"', 'host=db password=example,tail user=test'])
def test_truncated_outer_json_keeps_decoded_string_masking(value):
    result = mask_text(json.dumps({'message': value})[:-1])
    assert 'example' not in result and 'secret' not in result and 'tail' not in result


@pytest.mark.parametrize('source,expected', [
    ('[ERROR] password=example', '[ERROR] password=***'),
    ("{'password':'example'}", "{'password':'***'}"),
    ('[123] password=example', '[123] password=***'),
])
def test_non_json_braces_and_prefixes_keep_free_text_masking(source, expected):
    assert mask_text(source) == expected
