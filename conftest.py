"""``src/`` 를 임포트 경로에 추가해 ``coordinator`` / ``executor`` 로 임포트할 수 있게 한다.

저장소 루트도 함께 추가한다 — 커스텀 쿼리 함수(``customs.query_funcs.*``)는
src 밖(루트)에 있기 때문이다.
"""

import os
import sys

_ROOT = os.path.dirname(__file__)
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "src"))
