"""S3 업로더 어댑터 — executor 가 로컬 CSV 를 S3(호환) 오브젝트 스토어에 올리고 지운다.

``s3_stage`` exec_mode 전용. boto3 는 **지연 임포트**한다(impyla/psycopg 관례와 동일 —
드라이버 없이도 coordinator·테스트·타 모드가 동작). ``endpoint_url`` 로 온프렘 S3 호환
스토어(MinIO/Ceph 등)를 지원하며, 에어갭 전제와 무관한 **내부 데이터 경로**다(외부 CDN 아님).

인터페이스는 얇게(``upload``/``delete``) 유지해, 나중에 다른 업로더(예: 커스텀 함수)로
교체하고 싶으면 이 어댑터만 갈아끼우면 되게 한다. 테스트는 같은 시그니처의 가짜 클라이언트를
백엔드에 주입한다(``FakeS3Client``).
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


class S3Client:
    """boto3 기반 S3 업로드/삭제 어댑터. 클라이언트는 첫 사용 때 지연 생성한다.

    설정 dict 키: ``bucket``(필수), ``endpoint_url``, ``region``, ``access_key``,
    ``secret_key``, ``use_ssl``. 자격증명이 비어 있으면 boto3 기본 자격증명 체인(환경변수/
    인스턴스 프로파일 등)을 따른다.
    """

    def __init__(self, config: dict | None = None):
        self.config = dict(config or {})
        self.bucket = self.config.get("bucket") or ""
        self._client = None
        self._lock = threading.Lock()  # 지연 생성 경합 방지(여러 task 스레드가 동시 첫 사용)

    def _get_client(self):
        """boto3 S3 클라이언트를 지연 생성해 재사용한다(스레드 안전)."""
        if self._client is not None:
            return self._client
        with self._lock:
            if self._client is not None:
                return self._client
            import boto3  # 지연 임포트(옵션 의존성 — requirements-executor.txt)

            kwargs: dict = {}
            if self.config.get("endpoint_url"):
                kwargs["endpoint_url"] = self.config["endpoint_url"]
            if self.config.get("region"):
                kwargs["region_name"] = self.config["region"]
            if self.config.get("access_key"):
                kwargs["aws_access_key_id"] = self.config["access_key"]
            if self.config.get("secret_key"):
                kwargs["aws_secret_access_key"] = self.config["secret_key"]
            if "use_ssl" in self.config:
                kwargs["use_ssl"] = bool(self.config["use_ssl"])
            logger.debug(
                "S3 클라이언트 생성: endpoint=%s region=%s bucket=%s",
                self.config.get("endpoint_url") or "(기본)",
                self.config.get("region") or "(기본)", self.bucket,
            )
            self._client = boto3.client("s3", **kwargs)
            return self._client

    def upload(self, local_path: str, key: str) -> None:
        """로컬 파일을 ``s3://<bucket>/<key>`` 로 업로드한다(multipart 는 boto3 가 자동)."""
        if not self.bucket:
            raise ValueError("s3.bucket 이 설정되지 않아 s3_stage 업로드를 할 수 없습니다.")
        logger.debug("S3 업로드: %s → s3://%s/%s", local_path, self.bucket, key)
        self._get_client().upload_file(local_path, self.bucket, key)

    def delete(self, key: str) -> None:
        """``s3://<bucket>/<key>`` 객체를 삭제한다(정리용)."""
        if not self.bucket:
            return
        logger.debug("S3 삭제: s3://%s/%s", self.bucket, key)
        self._get_client().delete_object(Bucket=self.bucket, Key=key)


def build_s3_client(config: dict | None):
    """설정에서 S3 클라이언트를 만든다. ``bucket`` 이 비어 있으면 None(=s3_stage 미구성)."""
    if not (config and config.get("bucket")):
        return None
    return S3Client(config)
