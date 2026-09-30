"""질문에 답하며 ``bin/load-test run`` 명령행을 구성하는 대화형 마법사다(``bin/load-test wizard``).

옵션이 서른 개 가까이 되다 보니 처음 쓰는 사람은 무엇을 꼭 줘야 하는지부터 헤맨다. 마법사는 꼭 필요한
것만 순서대로 묻고(대상 → 요청 → 부하 곡선 → 대기 → 자원 수집 → 출력), 앞의 답에 따라 뒤의 질문을
고른다. 예를 들어 sync 를 고르면 폴링 규칙은 묻지 않고, ``/jobs`` 규칙을 그대로 쓰겠다고 하면 폴링
세부 항목을 건너뛴다.

**마법사는 옵션 dict 를 만들 뿐 실행 경로를 따로 두지 않는다.** 끝에서 그 dict 를
:func:`cli.opts_to_argv` 로 명령행 인자로 바꿔 보여 주고, 실행할 때도 그 인자를 :func:`cli.main` 에
그대로 넘긴다. 그래서 화면에 보인 명령과 실제로 실행된 설정이 어긋날 수 없고, 사용자는 보인 명령을
복사해 다음부터 마법사 없이 쓸 수 있다.

화면은 curses 가 아니라 한 줄씩 묻고 답하는 방식이다. 원격 셸이나 좁은 터미널, 로그를 남기는
``script`` 세션에서도 깨지지 않고, 입력을 파이프로 넣어 테스트할 수 있다. 모든 질문은 대괄호 안의
기본값을 Enter 로 받아들일 수 있고, ``?`` 를 치면 그 항목의 설명을 보여 준다.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, TextIO, Tuple

from . import cli, request, scenario
from .client import CoordinatorClient


class WizardAborted(Exception):
    """사용자가 입력을 끝냈다(EOF·Ctrl-C). 아무것도 실행하지 않고 빠져나간다."""


def _check_health(urls: Sequence[str]) -> List[Tuple[str, Optional[str]]]:
    """각 coordinator 의 ``/health`` 를 불러 (URL, 오류 또는 None) 목록을 돌려준다."""

    async def run() -> List[Tuple[str, Optional[str]]]:
        async with CoordinatorClient(urls, timeout=3.0) as client:
            out = []
            for url in client.base_urls:
                r = await client.health(url)
                ok = r.status_code == 200
                out.append((url, None if ok else (r.error or f"HTTP {r.status_code}")))
            return out

    return asyncio.run(run())


class Wizard:
    """질문 흐름을 담는다. 입출력과 접속 확인을 주입받아 테스트에서 대본대로 돌릴 수 있다."""

    def __init__(
        self,
        preset: Optional[Dict[str, Any]] = None,
        input_fn: Callable[[str], str] = input,
        out: Optional[TextIO] = None,
        health_check: Optional[Callable[[Sequence[str]], List[Tuple[str, Optional[str]]]]] = None,
        runner: Optional[Callable[[List[str]], int]] = None,
    ) -> None:
        self.opts: Dict[str, Any] = dict(preset or {})
        self._input = input_fn
        self.out = out or sys.stderr
        self._health = health_check if health_check is not None else _check_health
        self._run = runner or cli.main

    # ───────── 입력 도우미 ─────────

    def say(self, text: str = "") -> None:
        print(text, file=self.out)
        self.out.flush()

    def _read(self, prompt: str) -> str:
        self.out.flush()
        try:
            return self._input(prompt)
        except (EOFError, KeyboardInterrupt):
            raise WizardAborted() from None

    def ask(self, question: str, default: Any = None, *, help: str = "",
            convert: Optional[Callable[[str], Any]] = None, allow_empty: bool = False) -> Any:
        """한 줄 답을 받는다. 빈 답은 기본값, ``?`` 는 설명, ``-`` 는 '없음'(allow_empty 일 때)이다.

        convert 가 ValueError 를 던지면 사유를 보여 주고 다시 묻는다.
        """
        shown = "" if default in (None, "") else f" [{default}]"
        while True:
            raw = self._read(f"  {question}{shown}: ").strip()
            if raw == "?":
                self.say(_indent(help or "설명이 없습니다.", "    "))
                continue
            if raw == "":
                if default in (None, ""):
                    if allow_empty:
                        return None
                    self.say("    값을 입력해 주세요. (설명은 ?)")
                    continue
                raw = str(default)
            if raw == "-" and allow_empty:
                return None
            if convert is None:
                return raw
            try:
                return convert(raw)
            except ValueError as exc:
                self.say(f"    {exc}")

    def yes_no(self, question: str, default: bool, help: str = "") -> bool:
        def conv(v: str) -> bool:
            v = v.lower()
            if v in ("y", "yes", "예", "ㅛ"):
                return True
            if v in ("n", "no", "아니오", "ㅜ"):
                return False
            raise ValueError("y 또는 n 으로 답해 주세요.")
        return self.ask(f"{question} (y/n)", "y" if default else "n", help=help, convert=conv)

    def choose(self, question: str, options: Sequence[Tuple[str, str]], default: str,
               help: str = "") -> str:
        """번호로 고른다. options 는 (값, 설명) 목록이고 값이나 번호 어느 쪽으로 답해도 된다."""
        self.say(f"  {question}")
        width = max(len(v) for v, _ in options)
        for i, (value, label) in enumerate(options, 1):
            mark = "*" if value == default else " "
            self.say(f"   {mark}{i}) {value:<{width}}  {label}")
        default_no = next((str(i) for i, (v, _) in enumerate(options, 1) if v == default), "1")
        values = [v for v, _ in options]

        def conv(raw: str) -> str:
            if raw.isdigit() and 1 <= int(raw) <= len(options):
                return values[int(raw) - 1]
            if raw in values:
                return raw
            raise ValueError(f"1~{len(options)} 중 하나를 골라 주세요.")
        return self.ask("번호", default_no, help=help, convert=conv)

    def section(self, title: str) -> None:
        self.say(f"\n── {title} " + "─" * max(0, 44 - len(title)))

    # ───────── 질문 흐름 ─────────

    def run(self) -> int:
        self.say("부하 테스트 명령을 구성합니다. Enter 는 [기본값], ? 는 설명, Ctrl-D 는 중단입니다.")
        try:
            while True:
                self.ask_target()
                spec = self.ask_request()
                stages = self.ask_load()
                self.ask_waits(spec)
                self.ask_sampling()
                self.ask_outputs()
                action = self.finish(spec, stages)
                if action == "restart":
                    self.say("\n지금까지의 답을 기본값으로 두고 처음부터 다시 묻습니다.")
                    continue
                return action
        except WizardAborted:
            self.say("\n마법사를 중단했습니다. 아무것도 실행하지 않았습니다.")
            return cli.EXIT_CONFIG

    def ask_target(self) -> None:
        self.section("1. 대상")
        current = self.opts.get("coordinator") or os.environ.get("LOAD_COORDINATOR") \
            or cli.DEFAULT_COORDINATOR
        if isinstance(current, (list, tuple)):
            current = ",".join(current)

        def conv(raw: str) -> List[str]:
            urls = [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
            for u in urls:
                if not u.startswith(("http://", "https://")):
                    raise ValueError(f"http:// 또는 https:// 로 시작해야 합니다: {u}")
            if not urls:
                raise ValueError("주소를 하나 이상 입력해 주세요.")
            return urls
        urls = self.ask("coordinator 주소(여러 대면 쉼표로)", current, convert=conv, help=(
            "요청을 보낼 coordinator 의 base URL 입니다. 여러 대를 주면 요청을 라운드로빈으로\n"
            "나눠 보내고, 상태 폴링은 그 요청을 받은 coordinator 로 보냅니다."))
        self.opts["coordinator"] = urls
        for url, err in self._health(urls):
            if err is None:
                self.say(f"    연결 확인: {url} 정상")
            else:
                self.say(f"    연결 실패: {url} ({err}) — 설정은 계속 구성할 수 있습니다")

    def ask_request(self) -> request.RequestSpec:
        self.section("2. 요청")
        cur_type = self.opts.get("type") or request.default_type(
            str(self.opts.get("path") or "/jobs"), None)
        rtype = self.choose("요청이 끝나는 방식", (
            ("async", "접수 응답을 받은 뒤 상태 URL 을 폴링해 완료 확인 (/jobs)"),
            ("sync", "응답을 받는 순간 완료 (/query-execute, dry_run 등)"),
        ), cur_type, help=(
            "async 는 /jobs 처럼 202 로 바로 돌아오고 실제 처리는 뒤에서 도는 요청입니다.\n"
            "도구가 상태 URL 을 폴링해 종료 상태를 확인하므로 제출 TPS 와 완료 TPS 가 따로 나옵니다.\n"
            "sync 는 응답이 곧 결과인 요청으로, TPS 와 응답 지연 하나씩만 나옵니다."))
        self.opts["type"] = rtype

        # type 을 바꿨으면 이전 경로가 새 type 과 맞지 않을 수 있어 type 의 대표 경로를 기본으로 둔다.
        prev_path = self.opts.get("path")
        if prev_path and rtype == cur_type:
            default_path = prev_path
        else:
            default_path = "/jobs" if rtype == "async" else "/query-execute"

        def conv_path(raw: str) -> str:
            if not raw.startswith("/"):
                raise ValueError("경로는 / 로 시작해야 합니다. 예: /jobs")
            return raw
        self.opts["path"] = self.ask("경로", default_path, convert=conv_path)

        def conv_method(raw: str) -> str:
            m = raw.upper()
            if m not in request.METHODS:
                raise ValueError(f"{'/'.join(request.METHODS)} 중 하나여야 합니다.")
            return m
        self.opts["method"] = self.ask("HTTP 메서드", self.opts.get("method") or "POST",
                                       convert=conv_method)
        is_jobs = self.opts["path"].rstrip("/") == "/jobs"

        body_required = is_jobs or self.opts["method"] in ("POST", "PUT", "PATCH")
        body = self._ask_body(required=is_jobs, suggest=body_required)

        self.opts.pop("dry_run_body", None)
        if rtype == "sync" and is_jobs and body is not None and not body.get("dry_run"):
            if self.yes_no("sync 로 /jobs 를 보내면 job 을 만들고 기다리지 않습니다. "
                           "본문에 dry_run=true 를 넣을까요?", True, help=(
                               "dry_run 은 executor 호출 없이 검증·분할 계획만 200 으로 돌려주므로\n"
                               "coordinator 의 검증·템플릿·분할 경로만 부하를 겁니다.")):
                self.opts["dry_run_body"] = True
        if rtype == "async" and body is not None and body.get("dry_run"):
            self.say("    본문이 dry_run 이라 job 이 만들어지지 않으므로 type 을 sync 로 바꿉니다.")
            self.opts["type"] = rtype = "sync"

        if rtype == "sync":
            for key in request.POLL_OPTION_MAP:
                self.opts.pop(key, None)
        if rtype == "async":
            self._ask_poll_rules(is_jobs)
            self.opts.pop("success_status", None)
            if is_jobs:
                self.opts["idempotency_key"] = self.yes_no(
                    "요청마다 Idempotency-Key 헤더를 붙일까요?",
                    bool(self.opts.get("idempotency_key")), help=(
                        "coordinator 의 요청 멱등 경로(키 선점·재생 확인)까지 부하에 넣습니다.\n"
                        "키는 요청마다 달라 재생은 일어나지 않습니다.")) or None
        else:
            self.opts.pop("idempotency_key", None)

            def conv_codes(raw: str) -> str:
                request.parse_status_codes(raw)
                return raw
            codes = self.ask("성공으로 볼 HTTP 코드", self.opts.get("success_status") or "2xx",
                             convert=conv_codes, help=(
                                 "이 코드가 아닌 응답은 오류로 셉니다. 예: 200 또는 200,201 또는 2xx.\n"
                                 "429 는 따로 세며 오류율에 넣지 않습니다."))
            self.opts["success_status"] = None if codes == "2xx" else codes

        try:
            spec = cli.build_request(self.opts)
        except cli.ConfigError as exc:
            self.say(f"    {exc} — 요청 항목을 다시 묻습니다.")
            return self.ask_request()
        self.say(f"    요청: {spec.describe()} | {cli.describe_body(spec.body)}")
        return spec

    def _ask_body(self, required: bool, suggest: bool) -> Optional[Dict[str, Any]]:
        loaded: Dict[str, Any] = {}

        def conv(raw: str) -> str:
            path = os.path.expanduser(raw)
            try:
                body = cli.load_body({"body": path, "path": self.opts.get("path")})
            except cli.ConfigError as exc:
                raise ValueError(str(exc)) from None
            loaded["body"] = body
            return path
        question = "요청 본문 파일(JSON/YAML)" + ("" if required else ", 없으면 -")
        path = self.ask(question, self.opts.get("body") if isinstance(self.opts.get("body"), str)
                        else None, convert=conv, allow_empty=not required, help=(
            "요청에 실어 보낼 JSON 입니다. 문자열 안의 ${vu}·${iter}·${seq}·${uuid}·${run_id}·\n"
            "${now:%Y%m%d}·${choice:a,b}·${randint:1-9} 가 요청마다 치환됩니다.\n"
            "예: \"target_table\": \"dwtemp.load_${vu}\" 로 VU 마다 대상을 나눕니다."
            + ("" if suggest else "\nGET 처럼 본문이 없는 요청이면 - 를 입력합니다.")))
        self.opts["body"] = path
        body = loaded.get("body")
        if body is not None:
            self.say(f"    본문 요약: {cli.describe_body(body)}")
        return body

    def _ask_poll_rules(self, is_jobs: bool) -> None:
        custom = any(self.opts.get(k) not in (None, "") for k in request.POLL_OPTION_MAP)
        use_default = self.yes_no("폴링 규칙을 /jobs 기본값으로 쓸까요?", is_jobs and not custom,
                                  help=_poll_default_help())
        if use_default:
            for key in request.POLL_OPTION_MAP:
                self.opts.pop(key, None)
            return
        d = request.PollSpec()

        def conv_poll(raw: str) -> str:
            if "${id}" not in raw or not raw.startswith("/"):
                raise ValueError("/ 로 시작하고 ${id} 자리가 있어야 합니다. 예: /jobs/${id}/status")
            return raw
        self.opts["poll_path"] = self.ask("상태 URL", self.opts.get("poll_path") or d.path,
                                          convert=conv_poll,
                                          help="${id} 가 접수 응답에서 꺼낸 id 로 바뀝니다.")
        self.opts["poll_id_field"] = self.ask(
            "접수 응답의 id 필드", self.opts.get("poll_id_field") or d.id_field)
        self.opts["poll_status_field"] = self.ask(
            "상태 응답의 상태 필드", self.opts.get("poll_status_field") or d.status_field)
        terminal = self.ask("종료 상태(쉼표로)",
                            _join(self.opts.get("poll_terminal")) or ",".join(d.terminal),
                            convert=_nonempty_list, help="이 값 중 하나가 되면 폴링을 멈춥니다.")
        self.opts["poll_terminal"] = ",".join(terminal)

        def conv_success(raw: str) -> List[str]:
            items = _nonempty_list(raw)
            missing = [s for s in items if s not in terminal]
            if missing:
                raise ValueError(f"종료 상태에 없는 값입니다: {', '.join(missing)}")
            return items
        success = self.ask("그중 성공 상태(쉼표로)",
                           _join(self.opts.get("poll_success")) or
                           ",".join(s for s in d.success if s in terminal) or terminal[0],
                           convert=conv_success, help="나머지 종료 상태는 오류로 셉니다.")
        self.opts["poll_success"] = ",".join(success)
        cancel = self.ask("취소 URL(없으면 -)", self.opts.get("cancel_path") or d.cancel_path,
                          allow_empty=True, help=(
                              "테스트를 멈출 때 끝나지 않은 요청에 POST 로 보냅니다.\n"
                              "- 를 입력하면 취소하지 않고 그대로 둡니다."))
        self.opts["cancel_path"] = cancel if cancel else ""

    def ask_load(self) -> List[scenario.Stage]:
        self.section("3. 부하 곡선")
        mode = self.choose("VU(가상 사용자) 수를 늘리는 방식", (
            ("ramp", "0 에서 N 까지 일정하게 늘린 뒤 유지 (JMeter Thread Group)"),
            ("stages", "구간별 목표를 직접 지정 (계단·스파이크·감소)"),
        ), "stages" if self.opts.get("stages") else "ramp")
        if mode == "stages":
            for key in ("vus", "ramp_up", "duration"):
                self.opts.pop(key, None)

            def conv_stages(raw: str) -> str:
                scenario.parse_stages(raw)
                return raw
            self.opts["stages"] = self.ask("단계 '<시간>:<VU>,...'", self.opts.get("stages")
                                           or "30s:5,2m:5,1m:20,3m:20,30s:0",
                                           convert=conv_stages, help=(
                                               "각 단계는 그 시간 동안 목표 VU 수까지 선형으로 이동합니다.\n"
                                               "첫 단계는 0 VU 에서 출발합니다. 예: 30s:5,2m:5,1m:20,30s:0"))
            if self.yes_no("VU 당 요청 횟수에 상한을 둘까요?", bool(self.opts.get("iterations"))):
                self.opts["iterations"] = self.ask("VU 당 요청 횟수", self.opts.get("iterations")
                                                   or 10, convert=_positive_int)
            else:
                self.opts.pop("iterations", None)
        else:
            self.opts.pop("stages", None)
            self.opts["vus"] = self.ask("최대 VU 수", self.opts.get("vus") or 10,
                                        convert=_positive_int, help=(
                                            "동시에 요청을 보내는 가상 사용자 수입니다. VU 하나는 요청을 보내고\n"
                                            "완료될 때까지 기다린 뒤 다음을 보내므로 동시 진행 요청 수의 상한입니다."))
            self.opts["ramp_up"] = self.ask("ramp-up 시간", self.opts.get("ramp_up") or "1m",
                                            convert=_duration_text,
                                            help="0 에서 최대 VU 까지 늘리는 데 쓰는 시간입니다. 0 이면 한 번에 띄웁니다.")
            end = self.choose("끝내는 기준", (
                ("duration", "지정한 시간이 지나면 끝냄"),
                ("iterations", "VU 마다 정한 횟수를 채우면 끝냄"),
            ), "iterations" if self.opts.get("iterations") and not self.opts.get("duration")
                else "duration")
            if end == "duration":
                self.opts.pop("iterations", None)
                ramp = scenario.parse_duration(self.opts["ramp_up"])

                def conv_duration(raw: str) -> str:
                    if scenario.parse_duration(raw) < ramp:
                        raise ValueError("ramp-up 을 포함한 전체 시간이라 ramp-up 보다 길어야 합니다.")
                    return raw
                self.opts["duration"] = self.ask("전체 부하 시간(ramp-up 포함)",
                                                 self.opts.get("duration") or "10m",
                                                 convert=conv_duration)
            else:
                self.opts.pop("duration", None)
                self.opts["iterations"] = self.ask("VU 당 요청 횟수", self.opts.get("iterations")
                                                   or 10, convert=_positive_int)
        stages = cli.build_stages(self.opts)
        self.say("")
        self.say(_indent(cli.format_plan(stages, self.opts.get("iterations")), "    "))
        return stages

    def ask_waits(self, spec: request.RequestSpec) -> None:
        self.section("4. 대기와 타임아웃")
        self.opts["think_time"] = self.ask("요청 사이 쉬는 시간(think time)",
                                           self.opts.get("think_time") or "0",
                                           convert=_duration_text,
                                           help="요청이 끝나고 같은 VU 가 다음 요청을 보내기까지 쉬는 시간입니다.")
        if spec.is_async:
            self.opts["poll_interval"] = self.ask(
                "상태 폴링 간격", self.opts.get("poll_interval") or cli.DEFAULTS["poll_interval"],
                convert=_duration_text, help=(
                    "짧을수록 완료 시각이 정확하지만 VU 가 많으면 폴링만으로 coordinator 에 부하가 갑니다.\n"
                    "요청이 수십 초 이상 걸리면 5s 이상으로 늘려도 결과가 거의 같습니다."))
            self.opts["job_timeout"] = self.ask(
                "요청 하나를 기다리는 최대 시간", self.opts.get("job_timeout")
                or cli.DEFAULTS["job_timeout"], convert=_duration_text,
                help="넘기면 POLL_TIMEOUT 으로 세고 다음 요청으로 넘어갑니다.")
        else:
            for key in ("poll_interval", "job_timeout"):
                self.opts.pop(key, None)
        default_http = scenario.format_duration(cli.HTTP_TIMEOUT_DEFAULT[spec.type])
        http = self.ask("HTTP 타임아웃", self.opts.get("http_timeout") or default_http,
                        convert=_duration_text, help=(
                            "sync 요청은 이 시간이 곧 작업 타임아웃입니다(쿼리가 끝날 때까지 응답이 없으므로).\n"
                            "async 요청은 접수 응답만 기다리므로 짧아도 됩니다."))
        self.opts["http_timeout"] = None if http == default_http else http

    def ask_sampling(self) -> None:
        self.section("5. 서버 자원 수집")
        on = self.yes_no("테스트 중 서버 CPU/메모리를 수집할까요?", not self.opts.get("no_sample"),
                         help=("GET /cluster 로 coordinator 와 모든 executor 의 CPU·메모리·task 수를\n"
                               "주기적으로 모아 결과에 서버별 표로 붙입니다."))
        if not on:
            self.opts["no_sample"] = True
            for key in ("baseline", "sample_interval"):
                self.opts.pop(key, None)
            return
        self.opts.pop("no_sample", None)
        self.opts["baseline"] = self.ask("부하 전 기준선 수집 시간",
                                         self.opts.get("baseline") or cli.DEFAULTS["baseline"],
                                         convert=_duration_text,
                                         help="부하 전 자원 수준입니다. 결과의 '기준CPU'·'기준MEM' 열이 됩니다. 0 이면 생략합니다.")
        self.opts["sample_interval"] = self.ask(
            "수집 간격", self.opts.get("sample_interval") or cli.DEFAULTS["sample_interval"],
            convert=_duration_text)

    def ask_outputs(self) -> None:
        self.section("6. 결과 파일")
        stamp = datetime.now().strftime("%Y%m%d-%H%M")
        self.opts["out"] = self.ask("결과 JSON 파일(없으면 -)", self.opts.get("out")
                                    or f"load-{stamp}.json", allow_empty=True,
                                    help="설정·요약·시계열·자원 표본을 모두 담습니다. 두 실행을 비교할 때 씁니다.")
        self.opts["csv"] = self.ask("초 단위 시계열 CSV(없으면 -)", self.opts.get("csv"),
                                    allow_empty=True)
        self.opts["samples_csv"] = self.ask("서버 자원 표본 CSV(없으면 -)",
                                            self.opts.get("samples_csv"), allow_empty=True)

    def finish(self, spec: request.RequestSpec, stages: List[scenario.Stage]) -> Any:
        argv = cli.opts_to_argv(self.opts)
        self.section("완성된 명령")
        print(cli.format_command(argv), file=sys.stdout)
        sys.stdout.flush()
        if cli.writes_data(spec):
            self.say("\n  ※ dry_run 이 아닌 /jobs 라 실제로 적재합니다. 실행 전에 한 번 더 확인합니다.")
        while True:
            action = self.choose("\n다음으로", (
                ("run", "지금 실행"),
                ("save", "시나리오 YAML 로 저장"),
                ("print", "명령만 보여 주고 끝내기"),
                ("restart", "처음부터 다시(지금 답이 기본값)"),
            ), "run")
            if action == "run":
                self.say("")
                return self._run(argv)
            if action == "print":
                return cli.EXIT_OK
            if action == "restart":
                return "restart"
            path = self.ask("저장할 파일", "load-scenario.yml")
            if os.path.exists(path) and not self.yes_no(f"{path} 가 이미 있습니다. 덮어쓸까요?",
                                                        False):
                continue
            text = cli.opts_to_scenario(self.opts, spec)
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            self.say(f"    저장했습니다: {path}")
            self.say(f"    다음부터는: {os.environ.get('PROG_NAME', 'bin/load-test')} run "
                     f"--scenario {path}")


# ───────── 변환 도우미 ─────────

def _positive_int(raw: str) -> int:
    try:
        n = int(raw)
    except ValueError:
        raise ValueError("정수를 입력해 주세요.") from None
    if n <= 0:
        raise ValueError("1 이상이어야 합니다.")
    return n


def _duration_text(raw: str) -> str:
    """시간 표기를 검증하고 입력한 글자 그대로 돌려준다(명령행에 사람이 쓴 모양대로 남긴다)."""
    scenario.parse_duration(raw)
    return raw


def _nonempty_list(raw: str) -> List[str]:
    items = request.split_list(raw)
    if not items:
        raise ValueError("하나 이상 입력해 주세요.")
    return items


def _join(value: Any) -> str:
    return ",".join(request.split_list(value))


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.splitlines())


def _poll_default_help() -> str:
    d = request.PollSpec()
    return (f"기본값은 이 저장소 /jobs 의 규칙입니다.\n"
            f"  상태 URL {d.path}, id 필드 {d.id_field}, 상태 필드 {d.status_field}\n"
            f"  종료 상태 {','.join(d.terminal)} 중 성공은 {','.join(d.success)}\n"
            f"  취소 URL {d.cancel_path}\n"
            "다른 비동기 API 에 쓸 때만 n 을 골라 직접 지정합니다.")


def run_wizard(scenario_path: Optional[str] = None) -> int:
    """``bin/load-test wizard`` 진입점이다. 터미널이 아니면 마법사를 쓸 수 없다."""
    if not sys.stdin.isatty():
        print("wizard 는 대화형 터미널에서만 쓸 수 있습니다. 비대화형이면 run 에 옵션을 직접 주세요.",
              file=sys.stderr)
        return cli.EXIT_CONFIG
    preset = cli.load_scenario(scenario_path) if scenario_path else None
    result = Wizard(preset).run()
    return result if isinstance(result, int) else cli.EXIT_OK
