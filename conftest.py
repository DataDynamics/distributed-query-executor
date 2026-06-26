"""저장소 루트를 ``coordinator`` / ``executor`` 로 임포트할 수 있게 한다."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
