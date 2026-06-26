"""properties 변수 치환을 지원하는 설정 로더.

config.properties(Java 스타일 key=value)와 config.yml 을 읽어,
YAML 값의 ${variable} 자리표시자를 properties 값으로 치환한다.
(argus-catalog backend의 config_loader 와 동일한 방식)
"""

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_VAR_PATTERN = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")

DEFAULT_CONFIG_DIR = Path("/etc/query-executor")


def load_properties(path: Path) -> dict[str, str]:
    """Java 스타일 .properties 파일을 dict 로 읽어들인다."""
    props: dict[str, str] = {}

    if not path.is_file():
        logger.debug("properties 파일을 찾을 수 없음: %s", path)
        return props

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue

            for sep in ("=", ":"):
                idx = line.find(sep)
                if idx >= 0:
                    key = line[:idx].strip()
                    value = line[idx + 1 :].strip()
                    props[key] = value
                    break

    return props


def _resolve_value(value: str, props: dict[str, str]) -> str:
    """${variable} 또는 ${variable:default} 자리표시자를 치환한다."""

    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        default_value = match.group(2)

        if var_name in props:
            return props[var_name]

        if default_value is not None:
            return default_value

        logger.warning("치환되지 않은 변수: ${%s}", var_name)
        return match.group(0)

    return _VAR_PATTERN.sub(replacer, value)


def _resolve_dict(data: dict[str, Any], props: dict[str, str]) -> dict[str, Any]:
    """dict 안의 ${variable} 자리표시자를 재귀적으로 치환한다."""
    resolved = {}
    for key, value in data.items():
        if isinstance(value, str):
            resolved[key] = _resolve_value(value, props)
        elif isinstance(value, dict):
            resolved[key] = _resolve_dict(value, props)
        elif isinstance(value, list):
            resolved[key] = [
                _resolve_value(item, props) if isinstance(item, str) else item for item in value
            ]
        else:
            resolved[key] = value
    return resolved


def load_yaml(path: Path) -> dict[str, Any]:
    """YAML 파일을 읽어 dict 로 반환한다."""
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
    """properties 변수로 YAML 을 치환하여 설정을 로드한다."""
    base_dir = config_dir or DEFAULT_CONFIG_DIR

    props_file = Path(properties_path) if properties_path else base_dir / properties_file
    yaml_config_file = Path(yaml_path) if yaml_path else base_dir / yaml_file

    props = load_properties(props_file)
    raw_config = load_yaml(yaml_config_file)

    if not raw_config:
        return {}

    return _resolve_dict(raw_config, props)
