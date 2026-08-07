"""coordinator 와 executor 가 공유하는 공용 코어 패키지다. 설정 로더와 설정, 로깅을 담는다."""

from core.version import __version__  # noqa: F401 — `from core import __version__` 편의 재수출
