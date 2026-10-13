"""properties 값으로 YAML의 ${변수:기본값}을 치환하는 설정 로더다.

환경별 값은 config.properties에, 구조는 config.yml에 둔다.
core.config는 load_config()의 반환값을 사용한다."""

# Python 3.9 호환: PEP 604 (``X | None``) 유니언을 함수 시그니처에서 쓰므로,
# 어노테이션 평가를 지연(문자열화)시켜 3.9 에서도 런타임 오류 없이 동작하게 한다.
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# ${변수명} 또는 ${변수명:기본값} 형태를 잡는 정규식이다. group(1) 은 변수명이고
# ('}' 와 ':' 를 제외한 문자들), group(2) 는 선택적인 기본값이다(':' 뒤부터 '}' 직전까지).
# 콜론이 없으면 group(2) 는 None 이다.
_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

# config_dir 인자가 없을 때 쓸 운영 기본 설정 디렉터리다.
DEFAULT_CONFIG_DIR = Path("/data1/distributed-query-executor/config")


def load_properties(path: Path) -> dict[str, str]:
    """properties 파일을 키·값 사전으로 읽는다. 파일이 없으면 빈 사전을 반환한다.

    빈 줄과 #/! 주석은 건너뛴다. 구분자는 =를 우선하고, 없으면 :를 사용한다.
    키·값의 앞뒤 공백을 제거하며 첫 구분자 뒤의 내용은 값으로 보존한다."""
    props: dict[str, str] = {}

    # 파일 부재는 오류가 아니라 정상 상황(설정이 선택적일 수 있음)이므로 debug 로만 남긴다.
    if not path.is_file():
        logger.debug("properties 파일을 찾을 수 없음: %s", path)
        return props

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # 빈 줄과 주석(# 또는 ! 로 시작하는 줄)은 건너뛴다.
            if not line or line.startswith("#") or line.startswith("!"):
                continue

            # '='를 우선하고 없으면 ':'로 나눈다. 둘 다 없으면 이 줄을 무시한다.
            for sep in ("=", ":"):
                idx = line.find(sep)
                if idx >= 0:
                    key = line[:idx].strip()
                    value = line[idx + 1 :].strip()
                    props[key] = value
                    break

    return props


def _resolve_value(value: str, props: dict[str, str]) -> str:
    """문자열의 자리표시자를 properties 값, 명시한 기본값 순으로 치환한다.

    둘 다 없으면 경고를 남기고 원본 자리표시자를 보존한다."""

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default_value = match.group(2)  # 콜론 표기가 없으면 None 이 된다

        if var_name in props:
            return props[var_name]

        if default_value is not None:
            return default_value

        # 값도 기본값도 없으면 치환에 실패했음을 알리되 원문(match.group(0))은 그대로 남긴다.
        logger.warning("치환되지 않은 변수: ${%s}", var_name)
        return match.group(0)

    return _VAR_PATTERN.sub(replacer, value)


def _resolve_dict(data: dict[str, Any], props: dict[str, str]) -> dict[str, Any]:
    """중첩 사전의 문자열 값을 치환한 새 사전을 반환한다.

    리스트는 문자열 원소만 치환하며, 나머지 타입과 리스트 안의 사전은 그대로 둔다."""
    resolved = {}
    for key, value in data.items():
        if isinstance(value, str):
            resolved[key] = _resolve_value(value, props)
        elif isinstance(value, dict):
            resolved[key] = _resolve_dict(value, props)
        elif isinstance(value, list):
            # 리스트는 문자열 원소만 치환하고 문자열이 아닌 원소는 원본을 유지한다.
            resolved[key] = [
                _resolve_value(item, props) if isinstance(item, str) else item for item in value
            ]
        else:
            resolved[key] = value
    return resolved


def load_yaml(path: Path) -> dict[str, Any]:
    """safe_load로 YAML을 읽는다. 파일이 없거나 최상위가 사전이 아니면 빈 사전을 반환한다."""
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
    """properties 값을 YAML에 적용한 설정 사전을 반환한다.

    명시한 yaml_path/properties_path가 디렉터리와 파일명 조합보다 우선한다.
    config_dir가 없으면 DEFAULT_CONFIG_DIR를 사용한다. YAML이 없으면 빈 사전을 반환한다."""
    base_dir = config_dir or DEFAULT_CONFIG_DIR

    # 명시한 경로가 있으면 그것을 쓰고, 없으면 디렉터리와 파일명을 합쳐 만든다.
    props_file = Path(properties_path) if properties_path else base_dir / properties_file
    yaml_config_file = Path(yaml_path) if yaml_path else base_dir / yaml_file

    props = load_properties(props_file)
    raw_config = load_yaml(yaml_config_file)

    # YAML 자체가 없으면 치환할 것도 없으므로 곧바로 돌려준다.
    if not raw_config:
        return {}

    return _resolve_dict(raw_config, props)
