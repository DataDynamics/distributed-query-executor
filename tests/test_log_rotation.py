"""일별 보관 파일만 정해진 개수로 유지하는지 실제 롤링으로 확인한다."""
import logging
import time

import pytest

from core.logging import _DailyFileHandler


@pytest.mark.parametrize("filename", ["service.log", "service-warn.log", "service.debug.log", "service"])
def test_rollover_removes_only_old_archives_for_its_own_basename(tmp_path, monkeypatch, filename):
    handler = _DailyFileHandler(tmp_path, filename, backup_count=2)
    stem = filename.rsplit(".", 1)[0] if "." in filename else filename
    extension = "." + filename.rsplit(".", 1)[1] if "." in filename else ".log"
    own_old = [f"{stem}_{day}{extension}" for day in ("20240101", "20240102")]
    unrelated = ["other_20240101.log", f"{stem}-warn_20240101{extension}",
                 f"{stem}_20240101{extension}.bak", f"{stem}_20241301{extension}",
                 f"{stem}_notadate{extension}"]
    for name in own_old + unrelated:
        (tmp_path / name).write_text("prior", encoding="utf-8")
    handler.utc = True
    handler.rolloverAt = 1704326400  # 2024-01-04 00:00 UTC
    monkeypatch.setattr(time, "time", lambda: 1704326401)
    try:
        handler.emit(logging.makeLogRecord({"msg": "current", "levelno": logging.INFO}))
        assert not (tmp_path / own_old[0]).exists()
        assert (tmp_path / own_old[1]).exists()
        assert (tmp_path / f"{stem}_20240103{extension}").exists()
        assert all((tmp_path / name).exists() for name in unrelated)
        assert (tmp_path / filename).read_text() == "current\n"
    finally:
        handler.close()


def test_zero_backup_count_keeps_all_archives(tmp_path):
    handler = _DailyFileHandler(tmp_path, "service.log", backup_count=0)
    (tmp_path / "service_20240101.log").write_text("prior")
    try:
        assert handler.getFilesToDelete() == []
    finally:
        handler.close()
