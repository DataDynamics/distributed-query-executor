"""``python -m tools.load`` 진입점이다(`bin/load-test` 가 부른다)."""

import sys

from .cli import main

sys.exit(main())
