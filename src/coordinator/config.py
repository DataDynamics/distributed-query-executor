"""Coordinator 설정 진입점(공용 코어 설정의 재노출).

coordinator 패키지는 별도 설정 구현을 두지 않고 공용 코어(core.config)에서 정의·로드한
Settings 클래스와 그 싱글턴 인스턴스 settings 를 그대로 가져와 다시 export 한다. 이렇게
하면 'from coordinator.config import settings' 같은 패키지-로컬 임포트 경로를 제공하면서도,
실제 설정 정의와 로딩 로직은 코어 한 곳으로 일원화된다.

설정 원본은 config.properties + config.yml 기반이며, dispatcher/app 등 다른 컴포넌트에서
쓰던 속성명(executors, max_dispatch_concurrency 등)을 동일하게 노출한다.
"""

from core.config import Settings, settings

# 패키지 외부에 공개할 심볼: 설정 클래스(Settings)와 로드된 싱글턴 인스턴스(settings).
__all__ = ["Settings", "settings"]
