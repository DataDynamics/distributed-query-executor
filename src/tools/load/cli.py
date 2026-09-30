"""`bin/load-test` 의 명령행이다. 명령행 인자와 시나리오 YAML 을 합쳐 부하 테스트를 실행한다.

우선순위는 명령행 > 시나리오 YAML > 기본값이다. 그래서 자주 쓰는 설정은 YAML 로 묶어 두고, 한 번만
바꿔 보고 싶은 값(VU 수 같은 것)은 명령행으로 덮어쓴다. YAML 의 키는 명령행 옵션 이름에서 앞의
``--`` 를 떼고 ``-`` 를 ``_`` 로 바꾼 것이다(``--ramp-up`` → ``ramp_up``). 요청 명세만은 읽기 좋게
``request:`` 블록(``type``·``method``·``path``·``body``·``success_status``·``poll:``)으로도 쓸 수 있다.

하위 명령은 셋이다.

- ``run`` 은 부하 테스트를 실행하고 요약을 출력한다.
- ``plan`` 은 요청을 보내지 않고 시간대별 VU 수만 보여 준다. ramp-up 곡선을 미리 확인할 때 쓴다.
- ``wizard`` 는 질문에 답하며 명령행을 구성한다(:mod:`tools.load.wizard`).

**요약은 stdout, 진행 상황과 안내는 stderr 로 낸다.** 요약을 파일로 받거나 파이프로 넘길 때 진행
줄이 섞이지 않게 하려는 것이다(tools.progress 와 같은 규칙).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import shlex
import signal
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import yaml

from ..progress import is_interactive
from . import report, request, scenario
from .client import CoordinatorClient
from .runner import LoadConfig, LoadRunner

DEFAULT_COORDINATOR = "http://127.0.0.1:8088"

#: 옵션 기본값. argparse 기본값을 None 으로 두고 여기서 채워야 "명령행에서 줬는가"를 구분해
#: YAML 값보다 우선시킬 수 있다. http_timeout 은 요청 type 에 따라 기본값이 달라 여기 두지 않는다.
DEFAULTS: Dict[str, Any] = {
    "ramp_up": "0",
    "think_time": "0",
    "poll_interval": "2s",
    "poll_jitter": 0.2,
    "job_timeout": "1h",
    "error_backoff": "1s",
    "drain_timeout": "5m",
    "on_stop": "cancel",
    "baseline": "10s",
    "sample_interval": "5s",
}

#: type 별 HTTP 타임아웃 기본값(초). async 는 접수 응답이 짧고 긴 대기는 --job-timeout 이 맡지만,
#: sync 는 요청 하나가 쿼리 실행 시간만큼 걸리므로 HTTP 타임아웃이 곧 작업 타임아웃이다.
HTTP_TIMEOUT_DEFAULT = {"async": 30.0, "sync": 300.0}

#: 종료 코드. 임계치 초과(3)를 따로 두어 CI 나 크론에서 성능 회귀를 판정할 수 있게 한다.
EXIT_OK, EXIT_CONFIG, EXIT_THRESHOLD, EXIT_INTERRUPTED = 0, 1, 3, 130

#: 옵션 dict 에는 있지만 명령행·YAML 로 되돌리지 않는 키
_INTERNAL_KEYS = ("command", "scenario")

#: 빈 문자열이 "없음"이 아니라 뜻을 갖는 키. cancel_path 를 비우면 "취소하지 않는다"는 설정이다.
_EMPTY_MEANINGFUL = ("cancel_path",)


class ConfigError(Exception):
    """설정이 잘못돼 시작할 수 없을 때 던진다. main 이 한 줄 메시지로 바꾼다."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=os.environ.get("PROG_NAME", "bin/load-test"),
        description="coordinator API 부하 테스트(ramp-up + 완료 확인 + 서버 CPU/메모리 수집)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예:\n"
            "  %(prog)s wizard                       # 질문에 답하며 명령 구성\n"
            "  %(prog)s run --body job.json --vus 20 --ramp-up 60s --duration 10m\n"
            "  %(prog)s run --type sync --path /query-execute --body q.json --vus 10 -d 5m\n"
            "  %(prog)s run --scenario stairs.yml --vus 40 --out result.json\n"
            "  %(prog)s plan --stages 1m:10,2m:10,1m:30\n"
        ),
    )
    sub = parser.add_subparsers(dest="command")
    for name, help_text in (("run", "부하 테스트를 실행한다"),
                            ("plan", "요청 없이 시간대별 VU 수만 보여 준다")):
        p = sub.add_parser(name, help=help_text,
                           formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        _add_arguments(p)
    w = sub.add_parser("wizard", help="질문에 답하며 명령행을 구성한다(대화형)")
    w.add_argument("--scenario", "-s", metavar="YAML",
                   help="이 시나리오의 값을 기본 답으로 채워 시작한다")
    return parser


def _add_arguments(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("대상")
    g.add_argument("--coordinator", "-c", action="append", default=None, metavar="URL",
                   help=f"coordinator 주소. 여러 번 주면 라운드로빈으로 나눠 보낸다 "
                        f"(기본: $LOAD_COORDINATOR 또는 {DEFAULT_COORDINATOR})")
    g.add_argument("--scenario", "-s", metavar="YAML", help="시나리오 파일(옵션 묶음)")

    g = p.add_argument_group("요청")
    g.add_argument("--type", "-t", choices=request.REQUEST_TYPES,
                   help="async=접수 뒤 상태 URL 폴링으로 완료 확인, sync=응답이 곧 완료 "
                        "(기본: /jobs 는 async, 그 밖과 dry_run 은 sync)")
    g.add_argument("--method", "-X", type=str.upper, choices=request.METHODS,
                   help="HTTP 메서드 (기본 POST)")
    g.add_argument("--path", "-p", help="요청 경로 (기본 /jobs)")
    g.add_argument("--body", "-b", metavar="FILE",
                   help="요청 본문(JSON/YAML). ${vu}·${iter}·${seq}·${uuid}·"
                        "${now:%%Y%%m%%d}·${choice:a,b}·${randint:1-9}·${run_id} 치환")
    g.add_argument("--success-status", metavar="CODES",
                   help="sync 에서 성공으로 볼 HTTP 코드 (예: 200 또는 200,201, 기본 2xx)")
    g.add_argument("--dry-run-body", action="store_true", default=None,
                   help="본문에 dry_run=true 를 넣는다(sync 로 coordinator 검증·분할만 부하)")
    g.add_argument("--idempotency-key", action="store_true", default=None,
                   help="요청마다 고유한 Idempotency-Key 헤더를 붙인다")
    g.add_argument("--header", "-H", action="append", default=None, metavar="K:V",
                   help="추가 HTTP 헤더(여러 번 가능)")

    g = p.add_argument_group("async 폴링 규칙 (기본값은 /jobs 규칙)")
    g.add_argument("--poll-path", help="상태 URL. ${id} 가 접수 응답의 id 로 바뀐다 "
                                       "(기본 /jobs/${id}/status)")
    g.add_argument("--poll-id-field", help="접수 응답에서 id 를 꺼낼 필드 (기본 job_id)")
    g.add_argument("--poll-status-field", help="상태 응답의 상태 필드 (기본 status)")
    g.add_argument("--poll-terminal", metavar="A,B",
                   help="종료 상태 목록 (기본 DONE,PARTIAL,FAILED,CANCELLED)")
    g.add_argument("--poll-success", metavar="A,B", help="성공 상태 목록 (기본 DONE)")
    g.add_argument("--cancel-path", help="멈출 때 쓰는 취소 URL, 빈 값이면 취소 안 함 "
                                         "(기본 /jobs/${id}/cancel)")

    g = p.add_argument_group("부하 곡선 (--vus 또는 --stages 중 하나)")
    g.add_argument("--vus", "-u", type=int, help="최대 VU(가상 사용자) 수")
    g.add_argument("--ramp-up", "-r", help="0 에서 --vus 까지 늘리는 시간 (기본 0)")
    g.add_argument("--duration", "-d", help="ramp-up 을 포함한 전체 부하 시간 (예: 10m)")
    g.add_argument("--stages", help="단계 목록 '<시간>:<VU>,...' (예: 30s:5,2m:5,1m:20,30s:0)")
    g.add_argument("--iterations", "-n", type=int, help="VU 당 요청 횟수 상한")
    g.add_argument("--think-time", help="요청이 끝나고 다음 요청까지 쉬는 시간 (기본 0)")

    g = p.add_argument_group("대기와 종료")
    g.add_argument("--poll-interval", help="async 상태 폴링 간격 (기본 2s)")
    g.add_argument("--poll-jitter", type=float, help="폴링 간격 ± 비율 (기본 0.2)")
    g.add_argument("--job-timeout", help="async 요청 하나를 기다리는 최대 시간 (기본 1h)")
    g.add_argument("--http-timeout",
                   help="HTTP 요청 타임아웃 (기본 async 30s, sync 5m — sync 는 이것이 작업 타임아웃)")
    g.add_argument("--reject-backoff",
                   help="429 뒤 쉬는 시간 (기본: 응답의 Retry-After, 없으면 1s)")
    g.add_argument("--error-backoff", help="4xx/5xx/연결 오류 뒤 쉬는 시간 (기본 1s)")
    g.add_argument("--drain-timeout", help="멈출 때 진행 중 요청을 기다리는 시간 (기본 5m)")
    g.add_argument("--on-stop", choices=("cancel", "abandon"),
                   help="drain 뒤에도 남은 async 요청: cancel=취소 요청, abandon=방치 (기본 cancel)")

    g = p.add_argument_group("서버 자원 수집")
    g.add_argument("--baseline", help="부하 전 기준선 수집 시간 (기본 10s, 0 이면 생략)")
    g.add_argument("--sample-interval", help="GET /cluster 수집 간격 (기본 5s)")
    g.add_argument("--no-sample", action="store_true", default=None, help="자원 수집을 끈다")
    g.add_argument("--no-refresh", action="store_true", default=None,
                   help="/cluster?refresh=false 로 coordinator 캐시값을 쓴다(수집 부하 최소화)")

    g = p.add_argument_group("출력")
    g.add_argument("--out", "-o", metavar="JSON", help="전체 결과 JSON 파일")
    g.add_argument("--csv", metavar="CSV", help="초 단위 시계열 CSV 파일")
    g.add_argument("--samples-csv", metavar="CSV", help="서버 자원 표본 CSV 파일")
    g.add_argument("--max-error-rate", type=float,
                   help="오류율(0~1)이 이 값을 넘으면 종료 코드 3")
    g.add_argument("--no-progress", action="store_true", default=None, help="진행 줄을 끈다")
    g.add_argument("--yes", "-y", action="store_true", default=None,
                   help="실제 적재 요청의 시작 확인을 건너뛴다")
    g.add_argument("--seed", type=int, help="난수 시드(치환·폴링 jitter 재현용)")


def _run_parser() -> argparse.ArgumentParser:
    """``run`` 하위 명령의 파서다. 옵션 dict 를 명령행으로 되돌릴 때 옵션 정의를 읽는 데 쓴다."""
    parser = build_parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    return sub.choices["run"]


def option_keys() -> List[str]:
    """``run`` 이 받는 옵션 키(dest) 목록이다. 시나리오 YAML 이 받는 키이기도 하다."""
    return [a.dest for a in _run_parser()._actions
            if a.option_strings and a.dest not in ("help",) + _INTERNAL_KEYS]


# ───────── 설정 병합 ─────────

def _load_structured(path: str) -> Any:
    """JSON 또는 YAML 파일을 읽는다. YAML 은 JSON 의 상위집합이라 하나로 읽어도 되지만,
    JSON 오류 메시지가 더 정확하므로 확장자가 .json 이면 json 으로 읽는다."""
    with open(path, encoding="utf-8") as f:
        text = f.read()
    if path.lower().endswith(".json"):
        return json.loads(text)
    return yaml.safe_load(text)


def load_scenario(path: str) -> Dict[str, Any]:
    """시나리오 YAML 을 평평한 옵션 dict 로 읽는다. 본문 경로는 시나리오 파일 기준으로 푼다."""
    try:
        data = _load_structured(path) or {}
    except (OSError, ValueError, yaml.YAMLError) as exc:
        raise ConfigError(f"시나리오 파일을 읽을 수 없습니다: {exc}") from None
    if not isinstance(data, dict):
        raise ConfigError("시나리오 파일의 최상위는 key: value 매핑이어야 합니다")
    out: Dict[str, Any] = {}
    known = set(option_keys())
    for k, v in data.items():
        key = str(k).replace("-", "_")
        if key == "request":
            try:
                out.update(request.flatten_request_block(v))
            except ValueError as exc:
                raise ConfigError(str(exc)) from None
        elif key in known:
            out[key] = v
        else:
            raise ConfigError(f"시나리오에 알 수 없는 키가 있습니다: {k}")
    body = out.get("body")
    if isinstance(body, str) and not os.path.isabs(body):
        out["body"] = os.path.join(os.path.dirname(os.path.abspath(path)), body)
    return out


def merge_options(args: argparse.Namespace) -> Dict[str, Any]:
    """명령행 > 시나리오 YAML > 기본값 순으로 합친 옵션 dict 를 만든다."""
    opts: Dict[str, Any] = dict(DEFAULTS)
    if getattr(args, "scenario", None):
        opts.update(load_scenario(args.scenario))
    for k, v in vars(args).items():
        if v is not None and k not in _INTERNAL_KEYS:
            opts[k] = v
    return opts


def _dur(opts: Dict[str, Any], key: str) -> Optional[float]:
    v = opts.get(key)
    if v is None or v == "":
        return None
    try:
        return scenario.parse_duration(v)
    except ValueError as exc:
        raise ConfigError(f"--{key.replace('_', '-')}: {exc}") from None


def build_stages(opts: Dict[str, Any]) -> List[scenario.Stage]:
    try:
        if opts.get("stages"):
            if opts.get("vus") or opts.get("duration"):
                raise ConfigError("--stages 와 --vus/--duration 은 함께 쓸 수 없습니다")
            stages = opts["stages"]
            if isinstance(stages, list):  # YAML 에서 [{duration: 30s, target: 5}, ...] 형태
                return [scenario.Stage(scenario.parse_duration(s["duration"]), int(s["target"]))
                        for s in stages]
            return scenario.parse_stages(stages)
        if not opts.get("vus"):
            raise ConfigError("--vus 또는 --stages 가 필요합니다")
        duration = _dur(opts, "duration")
        if duration is None and not opts.get("iterations"):
            raise ConfigError("--duration 또는 --iterations 중 하나는 있어야 끝낼 수 있습니다")
        return scenario.stages_from_ramp(int(opts["vus"]), _dur(opts, "ramp_up") or 0.0, duration)
    except (ValueError, KeyError, TypeError) as exc:
        raise ConfigError(f"부하 곡선 설정 오류: {exc}") from None


def load_body(opts: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """요청 본문을 읽는다. 본문 없이 보내는 요청(GET 등)이면 None 이다.

    ``/jobs`` 는 본문 없이는 어차피 422 이므로 시작 전에 막는다.
    """
    body = opts.get("body")
    if body is None or body == "":
        if str(opts.get("path") or "/jobs").rstrip("/") == "/jobs":
            raise ConfigError("--body(요청 본문 파일)가 필요합니다")
        if opts.get("dry_run_body"):
            raise ConfigError("--dry-run-body 는 --body 와 함께 써야 합니다")
        return None
    if isinstance(body, str):
        try:
            body = _load_structured(body)
        except (OSError, ValueError, yaml.YAMLError) as exc:
            raise ConfigError(f"요청 본문을 읽을 수 없습니다: {exc}") from None
    if not isinstance(body, dict):
        raise ConfigError("요청 본문은 JSON 객체여야 합니다")
    if opts.get("dry_run_body"):
        body = dict(body, dry_run=True)
    try:
        scenario.validate_body(body)
    except ValueError as exc:
        raise ConfigError(f"요청 본문 치환 오류: {exc}") from None
    return body


def build_request(opts: Dict[str, Any]) -> request.RequestSpec:
    body = load_body(opts)
    try:
        return request.build_spec(opts, body)
    except ValueError as exc:
        raise ConfigError(f"요청 설정 오류: {exc}") from None


def build_config(opts: Dict[str, Any], spec: request.RequestSpec,
                 stages: List[scenario.Stage]) -> LoadConfig:
    if opts.get("iterations") is not None and int(opts["iterations"]) <= 0:
        raise ConfigError("--iterations 는 1 이상이어야 합니다")
    return LoadConfig(
        request=spec,
        stages=stages,
        iterations=int(opts["iterations"]) if opts.get("iterations") else None,
        think_time=_dur(opts, "think_time") or 0.0,
        poll_interval=max(0.05, _dur(opts, "poll_interval") or 2.0),
        poll_jitter=min(max(float(opts.get("poll_jitter") or 0.0), 0.0), 0.9),
        job_timeout=_dur(opts, "job_timeout") or 3600.0,
        reject_backoff=_dur(opts, "reject_backoff"),
        error_backoff=_dur(opts, "error_backoff") or 0.0,
        drain_timeout=_dur(opts, "drain_timeout") or 0.0,
        on_stop=opts.get("on_stop") or "cancel",
        idempotency_key=bool(opts.get("idempotency_key")),
        baseline=0.0 if opts.get("no_sample") else (_dur(opts, "baseline") or 0.0),
        sample_interval=max(1.0, _dur(opts, "sample_interval") or 5.0),
        seed=opts.get("seed"),
    )


def http_timeout(opts: Dict[str, Any], spec: request.RequestSpec) -> float:
    return _dur(opts, "http_timeout") or HTTP_TIMEOUT_DEFAULT[spec.type]


def coordinators(opts: Dict[str, Any]) -> List[str]:
    value = opts.get("coordinator") or os.environ.get("LOAD_COORDINATOR") or DEFAULT_COORDINATOR
    if isinstance(value, str):
        value = [v.strip() for v in value.split(",") if v.strip()]
    return list(value)


def parse_headers(opts: Dict[str, Any]) -> Dict[str, str]:
    raw = opts.get("header") or []
    if isinstance(raw, dict):  # YAML 에서는 매핑으로 줄 수 있다
        return {str(k): str(v) for k, v in raw.items()}
    headers = {}
    for h in raw:
        if ":" not in h:
            raise ConfigError(f"헤더는 'K:V' 형식이어야 합니다: {h!r}")
        k, v = h.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers


def describe_stages(stages: Sequence[scenario.Stage]) -> str:
    return " → ".join(f"{scenario.format_duration(s.duration)}:{s.target}" for s in stages)


def describe_body(body: Optional[Dict[str, Any]]) -> str:
    """확인 화면과 보고서에 쓸 본문 요약이다(무엇을 어디에 적재하는 요청인가)."""
    if not body:
        return "(본문 없음)"
    keys = ("template_id", "exec_mode", "target_table", "parallelism", "write_mode", "datasource")
    parts = [f"{k}={body[k]}" for k in keys if body.get(k) not in (None, "")]
    if body.get("dry_run"):
        parts.append("dry_run=true")
    return ", ".join(parts) or "(요약할 필드 없음)"


def writes_data(spec: request.RequestSpec) -> bool:
    """실제로 Greenplum 에 적재하는 요청인지다. 시작 확인을 받을지 가르는 기준이다."""
    return spec.is_async and spec.path.rstrip("/") == "/jobs"


# ───────── 옵션 dict ↔ 명령행·YAML ─────────

def opts_to_argv(opts: Dict[str, Any], command: str = "run") -> List[str]:
    """옵션 dict 를 명령행 인자 목록으로 되돌린다. 기본값과 같은 항목은 생략한다.

    마법사가 만든 옵션을 사람이 복사해 쓸 명령으로 보여 주고, 실행할 때도 이 인자를 다시 파싱해
    쓴다. 그래서 화면에 보인 명령과 실제로 실행된 설정이 어긋날 수 없다.
    """
    argv = [command]
    for action in _run_parser()._actions:
        if not action.option_strings or action.dest in ("help",) + _INTERNAL_KEYS:
            continue
        value = opts.get(action.dest)
        if value is None or value is False or value == []:
            continue
        if value == "" and action.dest not in _EMPTY_MEANINGFUL:
            continue
        if action.dest in DEFAULTS and str(value) == str(DEFAULTS[action.dest]):
            continue
        flag = next(s for s in action.option_strings if s.startswith("--"))
        if isinstance(action, argparse._StoreTrueAction):
            argv.append(flag)
        elif isinstance(action, argparse._AppendAction):
            for item in (value if isinstance(value, (list, tuple)) else [value]):
                argv += [flag, str(item)]
        else:
            if isinstance(value, (list, tuple)):
                value = ",".join(str(v) for v in value)
            argv += [flag, str(value)]
    return argv


def format_command(argv: Sequence[str]) -> str:
    """셸에 그대로 붙여 넣을 수 있는 명령 문자열이다. 옵션마다 줄을 바꿔 읽기 쉽게 한다."""
    prog = os.environ.get("PROG_NAME", "bin/load-test")
    parts: List[str] = [prog, argv[0]]
    i = 1
    while i < len(argv):
        tok = argv[i]
        if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
            parts.append(f"{tok} {shlex.quote(argv[i + 1])}")
            i += 2
        else:
            parts.append(tok)
            i += 1
    return " \\\n    ".join([" ".join(parts[:2])] + parts[2:])


def opts_to_scenario(opts: Dict[str, Any], spec: Optional[request.RequestSpec] = None) -> str:
    """옵션 dict 를 시나리오 YAML 문자열로 만든다. 요청 명세는 ``request:`` 블록으로 묶는다."""
    request_keys = set(request.REQUEST_YAML_MAP.values()) | set(request.POLL_YAML_MAP.values())
    data: Dict[str, Any] = {}
    if spec is not None:
        data["request"] = request.spec_to_yaml_block(spec, opts.get("body") or None)
    for key in option_keys():
        if key in request_keys:
            continue
        value = opts.get(key)
        if value is None or value == "" or value is False or value == []:
            continue
        if key in DEFAULTS and str(value) == str(DEFAULTS[key]):
            continue
        data[key] = value
    return yaml.safe_dump(data, allow_unicode=True, sort_keys=False)


# ───────── plan ─────────

def format_plan(stages: Sequence[scenario.Stage], iterations: Optional[int]) -> str:
    """단계 경계마다(그리고 긴 구간은 중간중간) 목표 VU 수를 표로 보여 준다."""
    total = scenario.total_duration(stages)
    finite = total if total != math.inf else sum(
        s.duration for s in stages if s.duration != math.inf)
    points = {0.0}
    t = 0.0
    for st in stages:
        if st.duration == math.inf:
            break
        for k in range(1, 5):
            points.add(t + st.duration * k / 4)
        t += st.duration
    lines = [f"단계: {describe_stages(stages)}",
             f"전체 부하 시간: {scenario.format_duration(total)}"
             + (f", VU 당 반복 {iterations}회" if iterations else ""),
             f"최대 VU: {scenario.max_vus(stages)}", ""]
    for p in sorted(points):
        if p > finite:
            continue
        v = scenario.target_vus(stages, p - 1e-6 if p > 0 else 0.0)
        lines.append(f"  {scenario.format_duration(round(p, 1)):>9}  {v:>5} VU  {'█' * min(v, 60)}")
    if total == math.inf:
        lines.append("  이후 반복 횟수를 채울 때까지 유지")
    return "\n".join(lines)


# ───────── run ─────────

def confirm(opts: Dict[str, Any], spec: request.RequestSpec, stages: List[scenario.Stage],
            coords: List[str]) -> bool:
    """실제 적재가 일어나는 요청이면 시작 전에 확인을 받는다.

    dry_run 이 아닌 /jobs 는 Greenplum 에 진짜로 적재하며, append 는 중복 적재된다. 읽기 전용인
    sync 요청(/query-execute 등)은 묻지 않는다.
    """
    if not writes_data(spec) or opts.get("yes"):
        return True
    msg = (
        "\n실제 적재 요청으로 부하 테스트를 시작합니다.\n"
        f"  대상 coordinator : {', '.join(coords)}\n"
        f"  요청             : {spec.describe()} | {describe_body(spec.body)}\n"
        f"  부하 곡선        : {describe_stages(stages)} (최대 VU {scenario.max_vus(stages)})\n"
        "  대상 테이블에 데이터가 반복 적재됩니다(append 는 중복).\n"
    )
    print(msg, file=sys.stderr)
    if not sys.stdin.isatty():
        print("비대화형 환경입니다. 진행하려면 --yes 를 주세요.", file=sys.stderr)
        return False
    try:
        answer = input("계속할까요? [y/N] ")
    except EOFError:
        return False
    return answer.strip().lower() in ("y", "yes")


async def _progress_loop(runner: LoadRunner, stop: asyncio.Event, interactive: bool) -> None:
    interval = 1.0 if interactive else 30.0
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
        if stop.is_set():
            break
        line = report.format_progress(runner)
        if interactive:
            sys.stderr.write("\r" + line + "\033[K")
        else:
            sys.stderr.write(line + "\n")
        sys.stderr.flush()
    if interactive:
        sys.stderr.write("\r\033[K")
        sys.stderr.flush()


async def execute(config: LoadConfig, coords: List[str], headers: Dict[str, str],
                  http_timeout: float, sample: bool, refresh: bool,
                  progress: bool, transport: Any = None) -> LoadRunner:
    """부하 테스트를 실행하고 끝난 러너를 돌려준다. 테스트는 transport 로 가짜 서버를 끼운다."""
    max_conn = scenario.max_vus(config.stages) * 2 + 10
    async with CoordinatorClient(coords, timeout=http_timeout, max_connections=max_conn,
                                 headers=headers, transport=transport) as client:
        runner = LoadRunner(config, client, sampler_enabled=sample)
        if runner.sampler:
            runner.sampler.refresh = refresh
        loop = asyncio.get_event_loop()
        installed = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, runner.request_stop,
                                        "사용자 중단" if sig == signal.SIGINT else "SIGTERM")
                installed.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass  # 메인 스레드가 아니거나 지원하지 않는 플랫폼
        stop_progress = asyncio.Event()
        progress_task = None
        if progress:
            progress_task = asyncio.ensure_future(
                _progress_loop(runner, stop_progress, is_interactive(sys.stderr)))
        try:
            await runner.run()
        finally:
            stop_progress.set()
            if progress_task is not None:
                await progress_task
            for sig in installed:
                loop.remove_signal_handler(sig)
        return runner


def cmd_plan(opts: Dict[str, Any]) -> int:
    stages = build_stages(opts)
    print(format_plan(stages, opts.get("iterations")))
    return EXIT_OK


def cmd_run(opts: Dict[str, Any]) -> int:
    stages = build_stages(opts)
    spec = build_request(opts)
    config = build_config(opts, spec, stages)
    coords = coordinators(opts)
    headers = parse_headers(opts)
    if not confirm(opts, spec, stages, coords):
        print("취소했습니다.", file=sys.stderr)
        return EXIT_CONFIG

    started = datetime.now()
    print(f"부하 테스트 시작 run_id={config.run_id} ({spec.describe()}, "
          f"{describe_stages(stages)}). Ctrl-C 한 번은 drain 후 종료, 두 번은 즉시 종료.",
          file=sys.stderr)
    runner = asyncio.run(execute(
        config, coords, headers,
        http_timeout=http_timeout(opts, spec),
        sample=not opts.get("no_sample"),
        refresh=not opts.get("no_refresh"),
        progress=not opts.get("no_progress"),
    ))

    meta = {
        "run_id": config.run_id,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "coordinators": coords,
        "request_type": spec.type,
        "request": spec.describe(),
        "scenario": f"{spec.describe()} | {describe_stages(stages)}"
                    + (f", VU 당 {config.iterations}회" if config.iterations else "")
                    + f" | {describe_body(spec.body)}",
        "options": {k: v for k, v in opts.items() if k not in ("body", "header")},
        "body": spec.body,
    }
    result = report.build_result(runner, meta)
    print(report.format_summary(result))
    if opts.get("out"):
        report.write_json(opts["out"], result)
        print(f"결과 JSON: {opts['out']}", file=sys.stderr)
    if opts.get("csv"):
        report.write_csv(opts["csv"], result["timeline"])
        print(f"시계열 CSV: {opts['csv']}", file=sys.stderr)
    if opts.get("samples_csv"):
        report.write_csv(opts["samples_csv"], result["samples"])
        print(f"자원 표본 CSV: {opts['samples_csv']}", file=sys.stderr)

    limit = opts.get("max_error_rate")
    if limit is not None and result["summary"]["error_rate"] > float(limit):
        print(f"오류율 {result['summary']['error_rate']:.2%} 가 한도 {float(limit):.2%} 를 넘었습니다.",
              file=sys.stderr)
        return EXIT_THRESHOLD
    if runner.stop_reason in ("사용자 중단", "SIGTERM"):
        return EXIT_INTERRUPTED
    return EXIT_OK


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return EXIT_OK
    try:
        if args.command == "wizard":
            from .wizard import run_wizard  # 대화형일 때만 필요하다
            return run_wizard(args.scenario)
        opts = merge_options(args)
        if args.command == "plan":
            return cmd_plan(opts)
        return cmd_run(opts)
    except ConfigError as exc:
        print(f"설정 오류: {exc}", file=sys.stderr)
        return EXIT_CONFIG
