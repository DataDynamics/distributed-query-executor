"""Coordinator 설정.

설정은 공용 코어(core.config)에서 로드한다. config.properties + config.yml 기반이며,
dispatcher/app 에서 쓰던 속성명(executors, max_dispatch_concurrency 등)을 그대로 노출한다.
"""

from core.config import Settings, settings

__all__ = ["Settings", "settings"]
