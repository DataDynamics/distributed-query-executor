"""DSN·JSON·자유 텍스트에서 알려진 비밀 필드를 가린다."""

from __future__ import annotations

import json
import re
from urllib.parse import unquote

_DSN_CRED_RE = re.compile(r'(://[^:/@\s"\']+:)[^@\s"\']*@')
_TRUNCATED_DSN_RE = re.compile(r'(postgres(?:ql)?://[^:/@\s"\']+:)[^/@\s"\']+(?=$|["\'])', re.I)
_SECRET_NAME_RE = re.compile(r'(?:password|passwd|pwd|secret|token|api[_-]?key|auth[_-]?token)', re.I)
_FIELD_RE = re.compile(
    r'''(?<![\w.-])(?P<name>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[\w%+-]+)\s*[:=]\s*'''
)
_JSON_DECODER = json.JSONDecoder()
_JSON_STRING_RE = re.compile(r'"(?:\\.|[^"\\])*(?P<closed>")?')

SENSITIVE_HEADERS = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key"}
)


def mask_dsn(dsn: str | None) -> str:
    """URI·keyword DSN의 비밀번호를 가리고 사용자·호스트·서식을 유지한다."""
    return _mask_text(dsn, keyword=bool(dsn and "://" not in dsn))


def _value_end(text: str, start: int, *, keyword: bool) -> tuple[int, str]:
    """값의 끝과 대체 문자열을 구한다. 닫히지 않은 값은 끝까지 가린다."""
    if start == len(text):
        return start, "***"
    quote = text[start] if text[start] in "\"'" else ""
    if quote:
        index = start + 1
        while index < len(text):
            if text[index] == "\\":
                index += 2
            elif text[index] == quote:
                return index + 1, quote + "***" + quote
            else:
                index += 1
        return len(text), quote + "***"
    if not keyword and text[start] in "[{":
        try:
            _, end = _JSON_DECODER.raw_decode(text, start)
            return end, '"***"'
        except (ValueError, RecursionError):
            return len(text), "***"
    delimiters = "" if keyword else ",}&]\"'"
    index = start
    while index < len(text):
        if text[index] == "\\":
            index += 2
        elif text[index].isspace() or text[index] in delimiters:
            break
        else:
            index += 1
    return min(index, len(text)), "***"


def mask_text(text: str | None) -> str:
    """중첩 JSON과 인용·이스케이프된 비밀값을 가린다. 잘린 값은 끝까지 숨긴다.

    알려진 필드와 URI 자격증명에 적용하며 임의의 민감 정보 탐지기는 아니다.
    비밀값 외의 공백과 구분자는 유지한다.
    """
    if not text:
        return ""
    try:
        json.loads(text)
    except json.JSONDecodeError as exc:
        # JSON으로 시작하다 잘린 경우만 구조 마스킹을 쓴다. 로그 접두사와
        # Python dict 표기는 자유 텍스트 규칙으로 처리한다.
        if exc.msg != "Extra data" and re.match(
            r'\s*(?:[\[{]\s*)+(?:"|-?\d|true\b|false\b|null\b|[\]}])', text
        ):
            return _mask_json(text)
    except RecursionError:
        return "***"
    else:
        return _mask_json(text)
    keyword = bool(text and re.match(r"\s*(?:host|user|dbname|port|password)\s*=", text))
    return _mask_text(text, keyword=keyword)


def _mask_json(text: str) -> str:
    """JSON 문자열을 해제해 내부 DSN도 가리고 원래 공백·구분자를 보존한다."""
    pieces = []
    cursor = 0
    for match in _JSON_STRING_RE.finditer(text):
        if match.start() < cursor:
            continue
        if not match.group("closed"):
            pieces.extend((text[cursor:match.start()], '"***'))
            cursor = len(text)
            break
        try:
            value = json.loads(match.group())
        except ValueError:
            pieces.extend((text[cursor:match.start()], '"***'))
            cursor = len(text)
            break
        after = match.end()
        while after < len(text) and text[after].isspace():
            after += 1
        if after < len(text) and text[after] == ":":
            if not _SECRET_NAME_RE.fullmatch(value):
                continue
            start = after + 1
            while start < len(text) and text[start].isspace():
                start += 1
            end, replacement = _value_end(text, start, keyword=False)
            if replacement == "***" and start < len(text) and text[start] not in "[{":
                replacement = '"***"'
            pieces.extend((text[cursor:start], replacement))
            cursor = end
        else:
            masked = mask_text(value)
            if masked != value:
                pieces.extend((text[cursor:match.start()], json.dumps(masked, ensure_ascii=False)))
                cursor = match.end()
    pieces.append(text[cursor:])
    return "".join(pieces)


def _mask_text(text: str | None, *, keyword: bool) -> str:
    if not text:
        return ""
    pieces = []
    cursor = 0
    for match in _FIELD_RE.finditer(text):
        if match.start() < cursor:
            continue
        name = match.group("name")
        if name.startswith('"'):
            try:
                name = json.loads(name)
            except ValueError:
                continue
        else:
            name = unquote(name.strip("'"))
        if not _SECRET_NAME_RE.fullmatch(name):
            continue
        end, replacement = _value_end(text, match.end(), keyword=keyword)
        # JSON의 숫자·불리언도 문자열로 대체해 유효한 JSON을 유지한다.
        if match.group("name").startswith('"') and ':' in match.group() and replacement == "***":
            if end < len(text) or text[match.end():end] in ("true", "false", "null"):
                replacement = '"***"'
        pieces.extend((text[cursor:match.end()], replacement))
        cursor = end
    pieces.append(text[cursor:])
    masked = "".join(pieces)
    masked = _DSN_CRED_RE.sub(r"\1***@", masked)
    return _TRUNCATED_DSN_RE.sub(r"\1***", masked)


def mask_header_value(name: str, value: str) -> str:
    """민감 HTTP 헤더는 통째로, 나머지는 알려진 비밀 필드만 가린다."""
    if name.lower() in SENSITIVE_HEADERS:
        return "***"
    return mask_text(value)
