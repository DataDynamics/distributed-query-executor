"""properties 변수 치환을 지원하는 설정 로더.

config.properties(Java 스타일 key=value)와 config.yml 을 읽어,
YAML 값 안의 ``${variable}`` / ``${variable:default}`` 자리표시자를 properties
값으로 치환한 최종 설정 dict 를 만든다. 이렇게 분리해 두면 환경별로 달라지는
값(호스트, 포트, 비밀번호 등)은 properties 에만 두고, 구조(YAML)는 공통으로
재사용할 수 있다. Spring Boot 의 ``${...:default}`` 관례를 그대로 흉내 낸 것이다.

core.config 모듈이 이 로더의 load_config() 를 호출해 원시 설정 dict 를 얻는다.
(argus-catalog backend의 config_loader 와 동일한 방식)
"""

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# ${변수명} 또는 ${변수명:기본값} 형태를 잡는 정규식.
#   group(1) = 변수명([^}:]+ : '}' 와 ':' 를 제외한 문자들)
#   group(2) = 기본값(선택적, ':' 뒤의 '}' 직전까지). 콜론이 없으면 None.
_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

# config_dir 인자가 주어지지 않았을 때 사용할 운영 기본 설정 디렉터리.
DEFAULT_CONFIG_DIR = Path("/etc/query-executor")


def load_properties(path: Path) -> dict[str, str]:
    """Java 스타일 .properties 파일을 읽어 key→value dict 로 반환한다.

    인자:
        path: 읽을 .properties 파일 경로.

    반환:
        파싱된 key/value dict. 파일이 없으면(선택적 설정일 수 있으므로) 빈 dict.

    파싱 규칙(Java properties 관례를 단순화한 형태):
        - 앞뒤 공백은 제거한다.
        - 빈 줄과 ``#``/``!`` 로 시작하는 주석 줄은 건너뛴다.
        - 구분자는 ``=`` 또는 ``:`` 둘 다 허용하며, 줄에서 **먼저 등장하는**
          구분자 한 개만 기준으로 key/value 를 나눈다. value 쪽에 구분자가 더
          들어 있어도(예: URL 의 ``:``) 잘리지 않게 하기 위함이다.
    """
    props: dict[str, str] = {}

    # 파일 부재는 오류가 아니라 정상 상황(설정이 선택적일 수 있음)이므로 debug 로만 남긴다.
    if not path.is_file():
        logger.debug("properties 파일을 찾을 수 없음: %s", path)
        return props

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # 빈 줄과 주석(# 또는 !)은 스킵.
            if not line or line.startswith("#") or line.startswith("!"):
                continue

            # '=' 와 ':' 중 줄에서 더 앞에 오는 구분자로 분리한다.
            # find 가 -1(없음)을 반환하면 다음 구분자를 시도하고, 둘 다 없으면 무시.
            for sep in ("=", ":"):
                idx = line.find(sep)
                if idx >= 0:
                    key = line[:idx].strip()
                    value = line[idx + 1 :].strip()
                    props[key] = value
                    break

    return props


def _resolve_value(value: str, props: dict[str, str]) -> str:
    """문자열 하나에 들어 있는 ``${variable}`` / ``${variable:default}`` 를 모두 치환한다.

    인자:
        value: 자리표시자를 포함할 수 있는 원본 문자열.
        props: 치환에 사용할 변수 사전.

    반환:
        모든 자리표시자가 치환된 문자열. 한 문자열에 자리표시자가 여러 개 있어도
        re.sub 가 전부 처리한다.

    치환 우선순위:
        1) props 에 변수명이 있으면 그 값.
        2) 없으면 ``:`` 뒤에 적힌 기본값(있을 경우).
        3) 둘 다 없으면 경고 로그를 남기고 **원본 자리표시자를 그대로 둔다**
           (예외로 죽이지 않고, 미치환 사실을 흔적으로 남겨 디버깅을 돕기 위함).
    """

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default_value = match.group(2)  # 콜론 표기가 없으면 None

        if var_name in props:
            return props[var_name]

        if default_value is not None:
            return default_value

        # 값도 기본값도 없는 경우: 치환 실패를 알리되 원문(match.group(0))을 보존.
        logger.warning("치환되지 않은 변수: ${%s}", var_name)
        return match.group(0)

    return _VAR_PATTERN.sub(replacer, value)


def _resolve_dict(data: dict[str, Any], props: dict[str, str]) -> dict[str, Any]:
    """중첩 dict 전체를 순회하며 문자열 값의 자리표시자를 재귀 치환한다.

    인자:
        data: YAML 에서 읽은 설정 dict(중첩 dict/list 포함 가능).
        props: 치환에 사용할 변수 사전.

    반환:
        구조는 동일하되 모든 문자열 값이 치환된 **새 dict**.

    타입별 처리:
        - str: 그 자리에서 치환.
        - dict: 한 단계 깊이 들어가 재귀 처리.
        - list: 원소 중 문자열만 치환하고 나머지 타입은 그대로 둔다
          (숫자/불리언 원소를 문자열로 바꾸지 않기 위함).
        - 그 외(int/bool/None 등): 치환 대상이 아니므로 그대로 복사.
    """
    resolved = {}
    for key, value in data.items():
        if isinstance(value, str):
            resolved[key] = _resolve_value(value, props)
        elif isinstance(value, dict):
            resolved[key] = _resolve_dict(value, props)
        elif isinstance(value, list):
            # 리스트는 문자열 원소만 치환하고 비문자열 원소는 원본 유지.
            resolved[key] = [
                _resolve_value(item, props) if isinstance(item, str) else item for item in value
            ]
        else:
            resolved[key] = value
    return resolved


def load_yaml(path: Path) -> dict[str, Any]:
    """YAML 파일을 안전하게 읽어 dict 로 반환한다.

    인자:
        path: 읽을 YAML 파일 경로.

    반환:
        파싱된 dict. 파일이 없거나, 비어 있거나, 최상위가 dict 가 아닌 경우
        (예: 리스트나 스칼라 한 개)에는 빈 dict 를 돌려준다 — 이후 단계가
        항상 dict 를 가정하고 동작하기 때문에 형태를 강제로 통일하는 것이다.

    임의 객체 생성을 막기 위해 ``yaml.safe_load`` 를 사용한다.
    """
    if not path.is_file():
        logger.debug("YAML 설정 파일을 찾을 수 없음: %s", path)
        return {}

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return data if isinstance(data, dict) else {}


def load_config(
    config_dir: Path | None = None,
    yaml_file: str = "config.yml",
    properties_file: str = "config.properties",
    yaml_path: Path | str | None = None,
    properties_path: Path | str | None = None,
) -> dict[str, Any]:
    """properties 변수로 YAML 을 치환하여 최종 설정 dict 를 만든다.

    이 모듈의 공개 진입점이다. properties 를 먼저 읽어 변수 사전을 만든 뒤,
    YAML 을 읽어 그 안의 자리표시자를 치환한다.

    인자:
        config_dir: 설정 파일들이 들어 있는 디렉터리. None 이면 DEFAULT_CONFIG_DIR.
        yaml_file: 디렉터리 안 YAML 파일명(yaml_path 가 없을 때만 사용).
        properties_file: 디렉터리 안 properties 파일명(properties_path 가 없을 때만 사용).
        yaml_path: YAML 파일의 명시적 전체 경로. 지정 시 config_dir/yaml_file 보다 우선.
        properties_path: properties 파일의 명시적 전체 경로. 지정 시 우선.

    반환:
        모든 자리표시자가 치환된 설정 dict. YAML 이 비어 있으면(설정 없음) 빈 dict.

    경로 결정 규칙:
        명시적 ``*_path`` 인자가 있으면 그것을 그대로 쓰고(테스트/임베디드에서
        임의 위치 파일을 지정하기 위함), 없으면 ``base_dir / *_file`` 로 조합한다.
    """
    base_dir = config_dir or DEFAULT_CONFIG_DIR

    # 명시 경로가 있으면 우선, 없으면 디렉터리+파일명으로 조합.
    props_file = Path(properties_path) if properties_path else base_dir / properties_file
    yaml_config_file = Path(yaml_path) if yaml_path else base_dir / yaml_file

    props = load_properties(props_file)
    raw_config = load_yaml(yaml_config_file)

    # YAML 자체가 없으면 치환할 것도 없으므로 조기 반환.
    if not raw_config:
        return {}

    return _resolve_dict(raw_config, props)
