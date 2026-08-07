"""S3 파일·디렉터리 조작 (업로드, 다운로드, 복사, 이동, 삭제, 목록, 내용 확인).

    bin/s3-ops ls       s3://dw-stage/orders/
    bin/s3-ops ls       s3://dw-stage/orders/ --summary
    bin/s3-ops upload   orders.csv s3://dw-stage/orders/
    bin/s3-ops download s3://dw-stage/orders/2026-08-01/ ./out/ --recursive
    bin/s3-ops head     s3://dw-stage/orders/2026-08-01/part-0.csv.gz
    bin/s3-ops rmdir    s3://dw-stage/orders/ --older-than 7d --yes

S3에는 디렉터리가 없다. 키가 ``a/b/c.csv`` 인 오브젝트가 있을 뿐이고, 콘솔이
슬래시를 보고 폴더처럼 보여줄 뿐이다. 그래서 이 스크립트에서는

- ``mkdir`` 은 ``a/b/`` 라는 빈 오브젝트를 하나 만든다(폴더 표시용 관례).
  파일을 올릴 때 상위 "디렉터리"를 미리 만들 필요는 없다.
- ``rmdir`` 은 그 접두사로 시작하는 오브젝트를 **전부** 지운다.

삭제는 되돌릴 수 없으므로 ``--yes`` 없이는 지울 목록을 보여주고 물어본다.

업로드는 8MB 가 넘으면 boto3 가 알아서 멀티파트로 나눠 올린다. 멀티파트 권한이 없는
계정에서는 ``CreateMultipartUpload`` 이 거부되는데, 그때는 단일 ``PutObject`` 로 한 번
더 시도한다. 처음부터 그렇게 하려면 ``--no-multipart`` 를 준다.

버킷과 자격증명은 이 저장소의 config/config.properties(s3.*)에서 자동으로 읽는다. 명령행으로 준
값이 항상 우선하고, 둘 다 없으면 환경변수나 IAM 역할을 따른다. 다른 파일을 쓰려면
``--config`` 를, 아예 읽지 않으려면 ``--no-config`` 를 준다.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import appconfig
from . import progress
from .progress import PhaseTimer

#: delete_objects 는 요청당 1000개까지만 받는다
DELETE_BATCH = 1000

#: boto3 가 멀티파트로 갈아타는 기본 크기
DEFAULT_MULTIPART_THRESHOLD = 8 * 1024 * 1024

#: PutObject 하나로 올릴 수 있는 최대 크기 (S3 제한). 그 위는 멀티파트뿐이다.
SINGLE_PUT_LIMIT = 5 * 1024**3

#: 설정의 s3 섹션에서 읽어 쓰는 키. 나머지 키는 무시한다.
S3_SETTINGS = (
    "bucket",
    "region",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "client_endpoint_url",
    "multipart_threshold",
)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """``s3://bucket/key`` 를 (버킷, 키)로 나눈다."""
    if not uri.startswith("s3://"):
        raise SystemExit(f"S3 경로는 s3://버킷/키 형식이어야 합니다: {uri!r}")
    rest = uri[len("s3://") :]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise SystemExit(f"버킷 이름이 없습니다: {uri!r}")
    return bucket, key


def as_directory(key: str) -> str:
    """디렉터리로 쓸 접두사는 슬래시로 끝나게 맞춘다."""
    return key if key.endswith("/") else key + "/"


#: 보고서에 표시할 구간 순서 (실제 실행 흐름대로)
PHASES = ("설정·클라이언트 준비", "명령 실행")

human = progress.human_bytes


def resolve_target(uri: str, default_bucket: Optional[str]) -> Tuple[str, str]:
    """경로를 (버킷, 키)로 푼다.

    ``s3://버킷/키`` 형태면 그대로 쓰고, 버킷 없이 키만 준 경우에는
    ``--bucket`` 이나 설정 파일의 버킷을 쓴다.
    """
    if uri.startswith("s3://"):
        return parse_s3_uri(uri)
    if not default_bucket:
        raise SystemExit(
            f"버킷을 알 수 없습니다: {uri!r}\n"
            "  s3://버킷/키 형식으로 주거나 --bucket 을 지정하세요."
        )
    return default_bucket, uri.lstrip("/")


def read_s3_settings(path: str, required: bool = True) -> Dict[str, Optional[str]]:
    """YAML 설정 파일의 s3 섹션에서 접속에 필요한 값만 읽는다.

    이 스크립트와 무관한 키는 건너뛴다. 그래서 다른 스크립트와 같은 설정 파일을
    나눠 쓸 수 있다.
    """
    return appconfig.load_section(Path(path), "s3", S3_SETTINGS, required=required)


def make_client(args: argparse.Namespace) -> Tuple[Any, Optional[str]]:
    """boto3 S3 클라이언트와 기본 버킷을 만든다.

    자격증명 우선순위는 명령행 > 설정 파일 > 환경변수/IAM 역할 순이다.
    ``--access-key`` 를 주지 않으면 boto3 기본 자격증명 체인이 그대로 동작한다.
    """
    path = appconfig.resolve_config_path(args)
    # --config 로 직접 지정한 파일에 s3 섹션이 없으면 오타일 가능성이 높다.
    # 기본 파일이라면 다른 스크립트용 설정만 들어 있을 수 있으니 넘어간다.
    settings = appconfig.load_section(
        path, "s3", S3_SETTINGS, required=bool(getattr(args, "config_dir", None))
    )

    # 명령행으로 준 값이 설정 파일보다 우선한다
    for key, given in (
        ("access_key_id", args.access_key),
        ("secret_access_key", args.secret_key),
        ("session_token", args.session_token),
        ("region", args.region),
        ("client_endpoint_url", args.endpoint),
    ):
        if given:
            settings[key] = given

    # 멀티파트 전환 크기는 upload 에만 있는 인자다. 명령행으로 주지 않았을 때만
    # 설정 파일 값을 채워 넣는다.
    if hasattr(args, "multipart_threshold") and args.multipart_threshold is None:
        args.multipart_threshold = settings["multipart_threshold"]

    # boto3 는 선택 의존성이다(requirements-executor.txt). 없을 때 트레이스백 대신
    # 설치 방법을 알려 준다 — gp-shell 이 psycopg 를 다루는 방식과 같다.
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit(
            "boto3 가 설치되어 있지 않습니다.\n"
            "    pip install boto3\n"
            "  또는 pip install -r requirements-executor.txt"
        ) from exc

    session = boto3.session.Session(
        aws_access_key_id=settings["access_key_id"],
        aws_secret_access_key=settings["secret_access_key"],
        aws_session_token=settings["session_token"],
        region_name=settings["region"],
    )
    client = session.client("s3", endpoint_url=settings["client_endpoint_url"] or None)
    return client, args.bucket or settings["bucket"]


def list_objects(client: Any, bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """접두사 아래 모든 오브젝트를 돌려준다.

    list_objects_v2 는 한 번에 1000개까지만 주므로 페이지네이터로 끝까지 훑는다.
    """
    paginator = client.get_paginator("list_objects_v2")
    return [
        obj
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])  # 결과가 비면 Contents 키가 없다
    ]


def delete_keys(
    client: Any,
    bucket: str,
    keys: Sequence[str],
    reporter: Optional[progress.Progress] = None,
) -> int:
    """키 목록을 1000개씩 묶어 지우고 실제로 지운 개수를 돌려준다."""
    deleted = 0
    processed = 0
    for start in range(0, len(keys), DELETE_BATCH):
        batch = keys[start : start + DELETE_BATCH]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = (response or {}).get("Errors") or []
        for error in errors:
            print(
                f"  삭제 실패: {error.get('Key')} ({error.get('Message')})",
                file=sys.stderr,
            )
        deleted += len(batch) - len(errors)
        processed += len(batch)
        if reporter is not None:
            reporter.update(processed)
    if reporter is not None:
        reporter.finish()
    return deleted


def confirm(question: str, assume_yes: bool) -> bool:
    """되돌릴 수 없는 작업 전에 확인을 받는다."""
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print("확인을 받을 수 없습니다. 실행하려면 --yes 를 주세요.", file=sys.stderr)
        return False
    return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")


# -- 명령 -------------------------------------------------------------------------


#: --older-than 이 받는 단위
DURATION_UNITS = {"m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str) -> float:
    """``24h`` / ``7d`` / ``90m`` 을 초로 바꾼다. 단위가 없으면 시간으로 본다."""
    value = text.strip().lower()
    unit = DURATION_UNITS.get(value[-1:], None)
    number = value[:-1] if unit else value
    try:
        amount = float(number)
    except ValueError:
        raise SystemExit(
            f"기간 형식이 잘못되었습니다: {text!r}\n"
            "  90m(분), 24h(시간), 7d(일), 2w(주) 처럼 주세요. 단위를 빼면 시간입니다."
        )
    return amount * (unit or 3600)


def older_than(objects: Iterable[Dict[str, Any]], seconds: float) -> List[Dict[str, Any]]:
    """지정한 기간보다 오래된 오브젝트만 고른다.

    LastModified 는 UTC 기준 timezone-aware datetime 이라 비교 대상도 tz 를 붙여야
    한다. naive datetime 과 비교하면 TypeError 가 난다.
    """
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return [o for o in objects if o["LastModified"] < cutoff]


def list_directories(client: Any, bucket: str, prefix: str) -> List[str]:
    """접두사 바로 아래 "디렉터리" 목록만 돌려준다.

    Delimiter 를 주면 그 아래는 접어서 CommonPrefixes 로 준다. 실행 단위가 몇 개
    남아 있는지 훑을 때 파일을 전부 나열하지 않아도 된다.
    """
    paginator = client.get_paginator("list_objects_v2")
    prefixes: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        prefixes.extend(entry["Prefix"] for entry in page.get("CommonPrefixes", []))
    return sorted(prefixes)


def print_summary(objects: Sequence[Dict[str, Any]]) -> None:
    """개수와 크기 분포. 파일이 고르게 나뉘었는지 보는 용도다.

    외부 테이블로 읽을 파일이라면 개수가 세그먼트 수보다 많아야 모든 세그먼트가
    일한다. docs/s3_external_table.md 참고.
    """
    sizes = [o["Size"] for o in objects]
    total = sum(sizes)
    print(f"파일 {len(sizes)}개, 합계 {human(total)}")
    print(
        f"최소 {human(min(sizes))} / 평균 {human(total / len(sizes))} / "
        f"최대 {human(max(sizes))}"
    )


def cmd_ls(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)

    if getattr(args, "dirs", False):
        prefixes = list_directories(client, bucket, as_directory(key) if key else "")
        if not prefixes:
            print(f"s3://{bucket}/{key} 아래에 디렉터리가 없습니다.")
            return 0
        for prefix in prefixes:
            print(prefix)
        print(f"\n{len(prefixes)}개")
        return 0

    objects = list_objects(client, bucket, key)
    if getattr(args, "older_than", None):
        objects = older_than(objects, parse_duration(args.older_than))
    if not objects:
        print(f"s3://{bucket}/{key} 아래에 오브젝트가 없습니다.")
        return 0

    if getattr(args, "summary", False):
        print_summary(objects)
        return 0

    for obj in sorted(objects, key=lambda o: o["Key"]):
        marker = "  <디렉터리 표시>" if obj["Key"].endswith("/") else ""
        print(f"{obj['LastModified']:%Y-%m-%d %H:%M}  {human(obj['Size']):>10}  {obj['Key']}{marker}")
    total = sum(o["Size"] for o in objects)
    print(f"\n{len(objects)}개, 합계 {human(total)}")
    return 0


def iter_upload_pairs(source: str, key: str, recursive: bool) -> List[Tuple[str, str]]:
    """(로컬 경로, S3 키) 쌍을 만든다."""
    path = Path(source)
    if path.is_dir():
        if not recursive:
            raise SystemExit(f"{source} 는 디렉터리입니다. --recursive 를 주세요.")
        prefix = as_directory(key) if key else ""
        pairs = []
        for local in sorted(p for p in path.rglob("*") if p.is_file()):
            # 원본 디렉터리 구조를 그대로 유지한다
            relative = local.relative_to(path).as_posix()
            pairs.append((str(local), prefix + relative))
        return pairs

    if not path.is_file():
        raise SystemExit(f"파일을 찾을 수 없습니다: {source}")
    # 대상이 디렉터리로 끝나면 파일 이름을 붙여준다
    return [(str(path), key + path.name if key.endswith("/") or not key else key)]


#: 멀티파트 개시가 막혔을 때 돌아오는 오류 코드들. S3 호환 스토리지는
#: 아예 구현하지 않았다는 뜻으로 NotImplemented 를 주기도 한다.
MULTIPART_DENIED_CODES = (
    "AccessDenied",
    "Forbidden",
    "403",
    "NotImplemented",
    "MethodNotAllowed",
)


def parse_size(text: Any) -> int:
    """``8MB`` / ``512KB`` / ``1.5GB`` 를 바이트로 바꾼다. 단위가 없으면 바이트다."""
    if isinstance(text, (int, float)):
        return int(text)
    value = str(text).strip().upper().replace("IB", "B")
    factor = 1
    for suffix, unit in (("KB", 1024), ("MB", 1024**2), ("GB", 1024**3)):
        if value.endswith(suffix):
            factor, value = unit, value[: -len(suffix)]
            break
    else:
        value = value[:-1] if value.endswith("B") else value
    try:
        return int(float(value.strip()) * factor)
    except ValueError:
        raise SystemExit(
            f"크기 형식이 잘못되었습니다: {text!r}\n"
            "  512KB, 8MB, 1.5GB 처럼 주세요. 단위를 빼면 바이트입니다."
        )


def transfer_config(threshold: Optional[int]) -> Any:
    """멀티파트 전환 크기를 바꾼 TransferConfig. 기본값 그대로면 None."""
    if threshold is None:
        return None
    from boto3.s3.transfer import TransferConfig

    return TransferConfig(multipart_threshold=threshold)


def is_multipart_denied(exc: BaseException) -> bool:
    """멀티파트 개시가 권한·미지원으로 막힌 오류인지 본다.

    ``upload_file`` 은 ClientError 를 S3UploadFailedError 문자열로 감싸버려서
    구조화된 오류 코드가 남지 않는다. 그래서 메시지를 본다.
    """
    text = str(exc)
    if "CreateMultipartUpload" not in text:
        return False
    return any(code in text for code in MULTIPART_DENIED_CODES)


def ensure_single_put_fits(local: str, size: int) -> None:
    """단일 PutObject 로 감당되는 크기인지 본다.

    5GB 를 넘으면 멀티파트 말고는 올릴 방법이 없다. 권한을 받아야 한다.
    """
    if size <= SINGLE_PUT_LIMIT:
        return
    raise SystemExit(
        f"{local} 은 {human(size)} 라서 멀티파트 없이는 올릴 수 없습니다"
        f" (PutObject 하나는 {human(SINGLE_PUT_LIMIT)} 까지).\n"
        "  s3:CreateMultipartUpload, s3:UploadPart, s3:CompleteMultipartUpload,\n"
        "  s3:AbortMultipartUpload 권한을 받거나 파일을 나눠서 올리세요."
    )


def put_single(
    client: Any, local: str, bucket: str, key: str, extra: Dict[str, str]
) -> None:
    """멀티파트를 쓰지 않고 PutObject 한 번으로 올린다.

    파일 객체를 그대로 넘기므로 내용을 통째로 메모리에 올리지는 않는다.
    """
    with open(local, "rb") as body:
        client.put_object(Bucket=bucket, Key=key, Body=body, **extra)


def upload_one(
    client: Any,
    local: str,
    bucket: str,
    key: str,
    extra: Dict[str, str],
    config: Any,
    single: bool,
) -> bool:
    """파일 하나를 올리고, 남은 파일도 단일 PutObject 로 갈지 여부를 돌려준다.

    ``upload_file`` 은 큰 파일을 알아서 멀티파트로 나눠 올린다. 그런데 멀티파트
    권한이 없는 계정에서는 개시부터 거부당한다. 그때 실패로 끝내지 않고 단일
    PutObject 로 한 번 더 시도한다. 개시가 막힌 것이므로 올라간 조각은 없다.

    한 번 막힌 계정은 다음 파일도 똑같이 막히니, 이후로는 곧장 PutObject 로 간다.
    """
    size = os.path.getsize(local)
    if single:
        ensure_single_put_fits(local, size)
        put_single(client, local, bucket, key, extra)
        return True

    try:
        client.upload_file(local, bucket, key, ExtraArgs=extra or None, Config=config)
    except Exception as exc:  # boto3 는 ClientError 를 S3UploadFailedError 로 감싼다
        if not is_multipart_denied(exc):
            raise
        print(
            f"멀티파트 업로드가 거부되었습니다: {exc}\n"
            "  단일 PutObject 로 다시 시도합니다. 계속 이렇게 쓸 거라면"
            " --no-multipart 를 주거나\n"
            "  설정의 s3.multipart_threshold 를 올려 두세요.",
            file=sys.stderr,
        )
        ensure_single_put_fits(local, size)
        put_single(client, local, bucket, key, extra)
        return True
    return False


def cmd_upload(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)
    pairs = iter_upload_pairs(args.source, key, args.recursive)
    if not pairs:
        print(f"올릴 파일이 없습니다: {args.source}")
        return 0

    extra: Dict[str, str] = {}
    if args.sse:
        extra["ServerSideEncryption"] = args.sse

    threshold = getattr(args, "multipart_threshold", None)
    config = transfer_config(parse_size(threshold) if threshold is not None else None)
    single = bool(getattr(args, "no_multipart", False))

    total_bytes = 0
    # 파일이 여러 개일 때만 진행 상황을 낸다. 하나뿐이면 결과 줄로 충분하다.
    reporter = progress.Progress(
        "업로드",
        total=len(pairs),
        unit="개",
        enabled=len(pairs) > 1 and not getattr(args, "no_progress", False),
    )
    for done, (local, target) in enumerate(pairs, 1):
        size = os.path.getsize(local)
        if args.dry_run:
            print(f"[예행] {local} → s3://{bucket}/{target} ({human(size)})")
            continue
        single = upload_one(client, local, bucket, target, extra, config, single)
        total_bytes += size
        reporter.update(done)
        print(f"{local} → s3://{bucket}/{target} ({human(size)})")
    reporter.finish()

    if not args.dry_run:
        print(f"\n{len(pairs)}개 업로드, 합계 {human(total_bytes)}")
    return 0


def iter_download_pairs(
    client: Any, bucket: str, key: str, destination: str, recursive: bool
) -> List[Tuple[str, str]]:
    """(S3 키, 로컬 경로) 쌍을 만든다.

    접두사를 받으면 그 아래 구조를 로컬에 그대로 편다. 파일 하나를 받을 때 대상이
    디렉터리면 원래 이름을 그대로 쓴다.
    """
    if recursive or key.endswith("/"):
        prefix = as_directory(key) if key else ""
        objects = list_objects(client, bucket, prefix)
        pairs = []
        for obj in sorted(objects, key=lambda o: o["Key"]):
            if obj["Key"].endswith("/"):
                continue  # 디렉터리 표시용 빈 오브젝트는 건너뛴다
            relative = obj["Key"][len(prefix):]
            pairs.append((obj["Key"], str(Path(destination) / relative)))
        return pairs

    target = Path(destination)
    if target.is_dir() or destination.endswith(os.sep):
        target = target / key.rsplit("/", 1)[-1]
    return [(key, str(target))]


def cmd_download(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)
    pairs = iter_download_pairs(client, bucket, key, args.destination, args.recursive)
    if not pairs:
        print(f"받을 파일이 없습니다: s3://{bucket}/{key}")
        return 0

    reporter = progress.Progress(
        "다운로드",
        total=len(pairs),
        unit="개",
        enabled=len(pairs) > 1 and not getattr(args, "no_progress", False),
    )
    downloaded = skipped = 0
    for done, (source_key, local) in enumerate(pairs, 1):
        if args.dry_run:
            print(f"[예행] s3://{bucket}/{source_key} → {local}")
            continue
        # 로컬 파일을 조용히 덮어쓰지 않는다. 되돌릴 수 없는 건 S3 쪽만이 아니다.
        if os.path.exists(local) and not args.force:
            print(f"이미 있어 건너뜁니다: {local}  (덮어쓰려면 --force)", file=sys.stderr)
            skipped += 1
            continue
        parent = os.path.dirname(local)
        if parent:
            os.makedirs(parent, exist_ok=True)
        client.download_file(bucket, source_key, local)
        downloaded += 1
        reporter.update(done)
        print(f"s3://{bucket}/{source_key} → {local} ({human(os.path.getsize(local))})")
    reporter.finish()

    if not args.dry_run:
        note = f"\n{downloaded}개 다운로드"
        if skipped:
            note += f", {skipped}개 건너뜀"
        print(note)
    return 0


def read_prefix_bytes(client: Any, bucket: str, key: str, max_bytes: int) -> bytes:
    """오브젝트 앞부분만 Range 로 받는다. 큰 파일도 전부 받지 않는다."""
    response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{max_bytes - 1}")
    return response["Body"].read()


def decompress_if_gzip(data: bytes) -> bytes:
    """gzip 이면 푼다. 앞부분만 받았으므로 끝이 잘려 있는 것이 정상이다."""
    if not data.startswith(b"\x1f\x8b"):
        return data
    import zlib

    # gzip 헤더를 이해하는 디코더. 잘린 스트림이라 마지막 블록에서 예외가 나는데,
    # 그때까지 푼 만큼은 쓸 수 있으므로 무시한다.
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        return decoder.decompress(data)
    except zlib.error:
        return decoder.flush() or b""


def cmd_head(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    """오브젝트 앞부분을 보여준다. 올린 파일이 제대로 됐는지 확인할 때 쓴다."""
    bucket, key = resolve_target(args.uri, bucket)
    if not key or key.endswith("/"):
        raise SystemExit(f"파일 하나를 지정하세요: {args.uri}")

    from botocore.exceptions import ClientError

    try:
        raw = read_prefix_bytes(client, bucket, key, args.max_bytes)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            print(f"없는 파일입니다: s3://{bucket}/{key}", file=sys.stderr)
            return 1
        raise

    data = raw if args.raw else decompress_if_gzip(raw)
    # 잘린 멀티바이트 문자가 끝에 걸릴 수 있으므로 replace 로 흘려보낸다
    text = data.decode(args.encoding, errors="replace")

    lines = text.splitlines()
    shown = lines if args.lines <= 0 else lines[: args.lines]
    for line in shown:
        print(line)

    print(
        f"\n앞 {human(len(raw))}만 받아 {len(shown)}줄 표시했습니다.",
        file=sys.stderr,
    )
    return 0


def cmd_exists(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    """오브젝트가 있는지 확인한다. 종료 코드로 알려주므로 셸 조건문에 바로 쓴다.

        if bin/s3-ops exists s3://dw-stage/orders/2026-08-01/_DONE; then ...

    없을 때(1)와 접근 권한이 없을 때(5)를 구분한다. 403 을 "없음" 으로 처리하면
    권한 문제를 데이터 문제로 착각하게 된다.
    """
    bucket, key = resolve_target(args.uri, bucket)
    if not key:
        raise SystemExit(f"확인할 키가 없습니다: {args.uri}")

    from botocore.exceptions import ClientError

    try:
        meta = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            if not args.quiet:
                print(f"없습니다: s3://{bucket}/{key}", file=sys.stderr)
            return 1
        if code == "403":
            print(f"권한이 없습니다: s3://{bucket}/{key}", file=sys.stderr)
            return 5
        raise

    if not args.quiet:
        size = human(meta.get("ContentLength", 0))
        modified = meta.get("LastModified")
        when = f"  {modified:%Y-%m-%d %H:%M}" if modified else ""
        print(f"있습니다: s3://{bucket}/{key}  {size}{when}")
    return 0


def iter_copy_pairs(
    client: Any, source_bucket: str, source_key: str, dest_key: str, recursive: bool
) -> List[Tuple[str, str]]:
    """(원본 키, 대상 키) 쌍을 만든다."""
    if recursive or source_key.endswith("/"):
        prefix = as_directory(source_key) if source_key else ""
        target = as_directory(dest_key) if dest_key else ""
        pairs = []
        for obj in sorted(list_objects(client, source_bucket, prefix), key=lambda o: o["Key"]):
            pairs.append((obj["Key"], target + obj["Key"][len(prefix):]))
        return pairs

    # 대상이 디렉터리로 끝나면 원본 이름을 붙인다
    if dest_key.endswith("/") or not dest_key:
        dest_key = dest_key + source_key.rsplit("/", 1)[-1]
    return [(source_key, dest_key)]


def copy_objects(
    client: Any,
    source_bucket: str,
    dest_bucket: str,
    pairs: Sequence[Tuple[str, str]],
    args: argparse.Namespace,
    verb: str,
) -> int:
    """서버측 복사로 오브젝트를 옮긴다. 실제로 복사한 개수를 돌려준다.

    copy_object 는 S3 안에서 처리되므로 받아서 다시 올리지 않는다. 큰 파일도
    네트워크를 타지 않는다.
    """
    reporter = progress.Progress(
        verb,
        total=len(pairs),
        unit="개",
        enabled=len(pairs) > 1 and not getattr(args, "no_progress", False),
    )
    copied = 0
    for done, (source_key, dest_key) in enumerate(pairs, 1):
        if source_bucket == dest_bucket and source_key == dest_key:
            print(f"원본과 대상이 같습니다. 건너뜁니다: {source_key}", file=sys.stderr)
            continue
        if args.dry_run:
            print(f"[예행] s3://{source_bucket}/{source_key} → s3://{dest_bucket}/{dest_key}")
            continue
        client.copy_object(
            Bucket=dest_bucket,
            Key=dest_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
        )
        copied += 1
        reporter.update(done)
        print(f"s3://{source_bucket}/{source_key} → s3://{dest_bucket}/{dest_key}")
    reporter.finish()
    return copied


def cmd_cp(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    source_bucket, source_key = resolve_target(args.source, bucket)
    dest_bucket, dest_key = resolve_target(args.uri, bucket)
    pairs = iter_copy_pairs(client, source_bucket, source_key, dest_key, args.recursive)
    if not pairs:
        print(f"복사할 오브젝트가 없습니다: s3://{source_bucket}/{source_key}")
        return 0

    copied = copy_objects(client, source_bucket, dest_bucket, pairs, args, "복사")
    if not args.dry_run:
        print(f"\n{copied}개 복사했습니다.")
    return 0


def cmd_mv(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    """복사한 뒤 원본을 지운다.

    S3 에는 이동이 없어서 복사 + 삭제로 처리한다. **복사가 끝난 것만** 지우므로,
    중간에 실패해도 아직 복사되지 않은 원본은 남는다.
    """
    source_bucket, source_key = resolve_target(args.source, bucket)
    dest_bucket, dest_key = resolve_target(args.uri, bucket)
    pairs = iter_copy_pairs(client, source_bucket, source_key, dest_key, args.recursive)
    if not pairs:
        print(f"옮길 오브젝트가 없습니다: s3://{source_bucket}/{source_key}")
        return 0

    copied = copy_objects(client, source_bucket, dest_bucket, pairs, args, "이동")
    if args.dry_run:
        print(f"[예행] 원본 {len(pairs)}개는 지우지 않았습니다.")
        return 0

    moved_keys = [src for src, _ in pairs[:copied]]
    deleted = delete_keys(client, source_bucket, moved_keys) if moved_keys else 0
    print(f"\n{copied}개 복사, 원본 {deleted}개 삭제했습니다.")
    return 0 if deleted == copied else 1


def cmd_buckets(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    """계정의 버킷을 나열한다.

    s3:ListAllMyBuckets 권한이 있어야 한다. 이 권한 없이 특정 버킷만 쓰는 계정도
    흔하므로, 목록이 비었다고 버킷이 없는 것은 아니다. 그때는 exists 로 개별
    버킷 접근을 확인하는 편이 낫다.
    """
    from botocore.exceptions import ClientError

    try:
        response = client.list_buckets()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("AccessDenied", "403"):
            print(
                "버킷 목록을 볼 권한이 없습니다(s3:ListAllMyBuckets).\n"
                "  쓰려는 버킷을 이미 안다면 exists 로 접근만 확인하세요.",
                file=sys.stderr,
            )
            return 5
        raise

    buckets = response.get("Buckets") or []
    if not buckets:
        print("버킷이 없습니다. 권한 범위가 좁은 계정일 수도 있습니다.")
        return 0

    for entry in sorted(buckets, key=lambda b: b["Name"]):
        line = entry["Name"]
        created = entry.get("CreationDate")
        if created:
            line = f"{created:%Y-%m-%d}  {line}"
        if args.show_region:
            # 버킷마다 한 번씩 더 부르므로 필요할 때만 켠다
            location = client.get_bucket_location(Bucket=entry["Name"])
            # us-east-1 은 역사적 이유로 None 을 돌려준다
            line += f"  ({location.get('LocationConstraint') or 'us-east-1'})"
        print(line)
    print(f"\n{len(buckets)}개")
    return 0


def cmd_mkdir(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)
    if not key:
        raise SystemExit("만들 디렉터리 경로가 없습니다.")
    marker = as_directory(key)

    if args.dry_run:
        print(f"[예행] 빈 오브젝트 생성: s3://{bucket}/{marker}")
        return 0

    client.put_object(Bucket=bucket, Key=marker, Body=b"")
    print(f"디렉터리 표시를 만들었습니다: s3://{bucket}/{marker}")
    print("  참고: S3에는 디렉터리가 없습니다. 파일을 올릴 때 미리 만들 필요는 없고,")
    print("        콘솔에서 빈 폴더로 보이게 하려는 용도입니다.")
    return 0


def cmd_rm(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)
    if not key or key.endswith("/"):
        raise SystemExit("파일 키를 지정하세요. 디렉터리를 지우려면 rmdir 을 쓰세요.")

    from botocore.exceptions import ClientError

    try:
        meta = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            print(f"없는 파일입니다: s3://{bucket}/{key}", file=sys.stderr)
            return 1
        raise

    print(f"삭제 대상: s3://{bucket}/{key} ({human(meta['ContentLength'])})")
    if args.dry_run:
        print("[예행] 지우지 않았습니다.")
        return 0
    if not confirm("지울까요?", args.yes):
        print("취소했습니다.")
        return 1

    client.delete_object(Bucket=bucket, Key=key)
    print("삭제했습니다.")
    return 0


def cmd_rmdir(client: Any, args: argparse.Namespace, bucket: Optional[str] = None) -> int:
    bucket, key = resolve_target(args.uri, bucket)
    # 접두사가 비면 버킷 전체가 지워진다. 실수를 막기 위해 반드시 막는다.
    if not key.strip("/"):
        raise SystemExit(
            "접두사가 비어 있습니다. 버킷 전체를 지우는 것을 막기 위해 거부합니다."
        )
    prefix = as_directory(key)

    objects = list_objects(client, bucket, prefix)
    window = getattr(args, "older_than", None)
    if window:
        objects = older_than(objects, parse_duration(window))
    if not objects:
        scope = f" ({window} 보다 오래된 것)" if window else ""
        print(f"s3://{bucket}/{prefix} 아래에 지울 오브젝트가 없습니다{scope}.")
        return 0

    total = sum(o["Size"] for o in objects)
    scope = f" ({window} 보다 오래된 것만)" if window else ""
    print(f"삭제 대상: s3://{bucket}/{prefix}{scope}")
    for obj in sorted(objects, key=lambda o: o["Key"])[:10]:
        print(f"  {obj['Key']}  ({human(obj['Size'])})")
    if len(objects) > 10:
        print(f"  ... 외 {len(objects) - 10}개")
    print(f"모두 {len(objects)}개, 합계 {human(total)}")

    if args.dry_run:
        print("[예행] 지우지 않았습니다.")
        return 0
    if not confirm(f"{len(objects)}개를 모두 지울까요?", args.yes):
        print("취소했습니다.")
        return 1

    deleted = delete_keys(
        client,
        bucket,
        [o["Key"] for o in objects],
        progress.Progress(
            "삭제",
            total=len(objects),
            unit="개",
            enabled=len(objects) > DELETE_BATCH and not getattr(args, "no_progress", False),
        ),
    )
    print(f"{deleted}개 삭제했습니다.")
    return 0 if deleted == len(objects) else 1


COMMANDS = {
    "ls": cmd_ls,
    "upload": cmd_upload,
    "download": cmd_download,
    "head": cmd_head,
    "exists": cmd_exists,
    "cp": cmd_cp,
    "mv": cmd_mv,
    "buckets": cmd_buckets,
    "mkdir": cmd_mkdir,
    "rm": cmd_rm,
    "rmdir": cmd_rmdir,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=os.environ.get("PROG_NAME", "bin/s3-ops"),
        description="S3 파일·디렉터리 조작 (업로드, 다운로드, 복사, 이동, 삭제, 목록, 내용 확인)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  bin/s3-ops ls       s3://dw-stage/orders/\n"
            "  bin/s3-ops ls       s3://dw-stage/orders/ --summary\n"
            "  bin/s3-ops upload   ./out/ s3://dw-stage/orders/2026-08-01/ --recursive\n"
            "  bin/s3-ops download s3://dw-stage/orders/2026-08-01/ ./out/ --recursive\n"
            "  bin/s3-ops head     s3://dw-stage/orders/2026-08-01/part-0.csv.gz\n"
            "  bin/s3-ops rm       s3://dw-stage/orders/orders.csv --yes\n"
            "  bin/s3-ops rmdir    s3://dw-stage/orders/ --older-than 7d --yes\n"
            "  bin/s3-ops mv       s3://dw-stage/orders/2026-08-01/ \\\n"
            "                      s3://dw-stage/archive/2026-08-01/ --recursive\n"
            "  bin/s3-ops exists   s3://dw-stage/orders/2026-08-01/_DONE\n"
            "\n"
            "  # 멀티파트 권한이 없어 CreateMultipartUpload 이 거부되는 계정\n"
            "  bin/s3-ops upload   big.csv.gz s3://dw-stage/orders/ --no-multipart\n"
            "\n"
            "  # --bucket 을 주면 s3:// 없이 키만 써도 됩니다\n"
            "  bin/s3-ops --bucket dw-stage ls impala/\n"
            "\n"
            "  # MinIO 등 S3 호환 스토리지\n"
            "  bin/s3-ops --endpoint http://minio:9000 --bucket dw-stage \\\n"
            "           --access-key minioadmin --secret-key minioadmin ls /\n"
            "\n"
            f"버킷과 자격증명은 {appconfig.DEFAULT_CONFIG} 의\n"
            "s3 섹션에서 자동으로 읽습니다. 아래 인자를 주면 그 값이 우선합니다.\n"
        ),
    )
    appconfig.add_config_arguments(parser)
    progress.add_progress_argument(parser)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="오류가 나면 전체 스택 트레이스를 출력합니다.",
    )
    connection = parser.add_argument_group("접속")
    connection.add_argument(
        "-b",
        "--bucket",
        help="기본 버킷. 지정하면 경로를 s3:// 없이 키만 줄 수 있습니다.",
    )
    connection.add_argument(
        "--access-key",
        metavar="KEY",
        help="AWS 액세스 키. 생략하면 AWS_ACCESS_KEY_ID 환경변수나 IAM 역할을 씁니다.",
    )
    connection.add_argument(
        "--secret-key",
        metavar="SECRET",
        help="AWS 시크릿 키. 명령행에 적으면 ps로 다른 사용자에게 보이므로, "
        "가능하면 AWS_SECRET_ACCESS_KEY 환경변수를 쓰세요.",
    )
    connection.add_argument(
        "--session-token", metavar="TOKEN", help="임시 자격증명(STS)을 쓸 때의 세션 토큰"
    )
    connection.add_argument("--region", help="AWS 리전")
    connection.add_argument(
        "--endpoint",
        "--endpoint-url",
        dest="endpoint",
        metavar="URL",
        help="S3 호환 스토리지 엔드포인트 (MinIO 등). 예: http://minio.example.com:9000",
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true", help="무엇을 할지만 보여주고 실행하지 않음"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    ls = sub.add_parser("ls", help="접두사 아래 오브젝트 목록")
    ls.add_argument("uri", help="s3://버킷/접두사")
    ls.add_argument(
        "--summary", action="store_true", help="목록 대신 개수와 크기 분포만 봅니다"
    )
    ls.add_argument(
        "--dirs", action="store_true", help="파일 대신 한 단계 아래 디렉터리만 봅니다"
    )
    ls.add_argument(
        "--older-than",
        metavar="기간",
        help="이 기간보다 오래된 것만 (예: 24h, 7d, 90m). 단위를 빼면 시간입니다.",
    )

    upload = sub.add_parser("upload", help="파일 또는 디렉터리 업로드")
    upload.add_argument("source", help="로컬 파일 또는 디렉터리")
    upload.add_argument("uri", help="s3://버킷/키 (또는 s3://버킷/접두사/)")
    upload.add_argument("-r", "--recursive", action="store_true", help="디렉터리 전체 업로드")
    upload.add_argument("--sse", help="서버측 암호화 (예: AES256)")
    upload.add_argument(
        "--no-multipart",
        action="store_true",
        help="멀티파트를 쓰지 않고 PutObject 하나로 올립니다. "
        "멀티파트 권한이 없는 계정에서 쓰며, 파일당 5GB 까지만 됩니다.",
    )
    upload.add_argument(
        "--multipart-threshold",
        metavar="크기",
        help="이 크기부터 멀티파트로 나눠 올립니다 (예: 64MB, 1GB). "
        f"기본은 boto3 기본값인 {human(DEFAULT_MULTIPART_THRESHOLD)} 입니다.",
    )

    download = sub.add_parser("download", help="파일 또는 접두사 전체 다운로드")
    download.add_argument("uri", help="s3://버킷/키 (또는 s3://버킷/접두사/)")
    download.add_argument("destination", help="저장할 로컬 파일 또는 디렉터리")
    download.add_argument(
        "-r", "--recursive", action="store_true", help="접두사 아래 전체 다운로드"
    )
    download.add_argument(
        "--force", action="store_true", help="이미 있는 로컬 파일을 덮어씁니다"
    )

    head = sub.add_parser("head", help="오브젝트 앞부분 보기 (.gz 자동 해제)")
    head.add_argument("uri", help="s3://버킷/키")
    head.add_argument(
        "-n", "--lines", type=int, default=10, help="보여줄 줄 수 (기본 10, 0이면 전부)"
    )
    head.add_argument(
        "--max-bytes",
        type=int,
        default=64 * 1024,
        help="받아올 최대 바이트 (기본 64KB). 압축 파일이면 푼 뒤가 더 길어집니다.",
    )
    head.add_argument("--encoding", default="utf-8", help="파일 인코딩 (기본 utf-8)")
    head.add_argument("--raw", action="store_true", help="gzip 자동 해제를 하지 않습니다")

    exists = sub.add_parser("exists", help="오브젝트가 있는지 확인 (종료 코드로 알림)")
    exists.add_argument("uri", help="s3://버킷/키")
    exists.add_argument(
        "-q", "--quiet", action="store_true", help="아무것도 출력하지 않고 종료 코드만"
    )

    cp = sub.add_parser("cp", help="S3 안에서 복사 (서버측 복사)")
    cp.add_argument("source", help="원본 s3://버킷/키 (또는 접두사/)")
    cp.add_argument("uri", help="대상 s3://버킷/키 (또는 접두사/)")
    cp.add_argument("-r", "--recursive", action="store_true", help="접두사 아래 전체")

    mv = sub.add_parser("mv", help="S3 안에서 이동 (복사 후 원본 삭제)")
    mv.add_argument("source", help="원본 s3://버킷/키 (또는 접두사/)")
    mv.add_argument("uri", help="대상 s3://버킷/키 (또는 접두사/)")
    mv.add_argument("-r", "--recursive", action="store_true", help="접두사 아래 전체")

    buckets = sub.add_parser("buckets", help="계정의 버킷 목록")
    # 전역 --region(AWS 리전 지정)과 헷갈리지 않도록 이름을 나눈다
    buckets.add_argument(
        "--show-region",
        action="store_true",
        help="버킷마다 리전도 함께 보여줍니다 (버킷당 호출이 한 번씩 늘어납니다)",
    )

    mkdir = sub.add_parser("mkdir", help="디렉터리 표시용 빈 오브젝트 생성")
    mkdir.add_argument("uri", help="s3://버킷/접두사/")

    rm = sub.add_parser("rm", help="파일 하나 삭제")
    rm.add_argument("uri", help="s3://버킷/키")
    rm.add_argument("-y", "--yes", action="store_true", help="확인 없이 삭제")

    rmdir = sub.add_parser("rmdir", help="접두사 아래 오브젝트 전체 삭제")
    rmdir.add_argument("uri", help="s3://버킷/접두사/")
    rmdir.add_argument("-y", "--yes", action="store_true", help="확인 없이 삭제")
    rmdir.add_argument(
        "--older-than",
        metavar="기간",
        help="이 기간보다 오래된 것만 지웁니다 (예: 24h, 7d). 크론 정리에 씁니다.",
    )

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    # 인자 없이 실행하면 무엇을 할 수 있는지 보여준다. 오류가 아니므로 0으로 끝낸다.
    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        return 0

    args = parser.parse_args(argv)
    timer = PhaseTimer(PHASES)

    with timer.measure("설정·클라이언트 준비"):
        client, bucket = make_client(args)

    try:
        with timer.measure("명령 실행"):
            return COMMANDS[args.command](client, args, bucket)
    except Exception as exc:
        # 크론 메일에 수백 줄짜리 스택 트레이스가 실리지 않게 한 줄로 줄인다.
        # 전체가 필요하면 --debug 로 본다.
        if args.debug:
            raise
        print(f"\n실패: {type(exc).__name__}: {exc}", file=sys.stderr)
        print("  전체 오류를 보려면 --debug 를 주세요.", file=sys.stderr)
        return 5
    finally:
        # 성공하든 실패하든 어디에 시간을 썼는지는 항상 남긴다
        timer.print_report()


if __name__ == "__main__":
    raise SystemExit(main())
