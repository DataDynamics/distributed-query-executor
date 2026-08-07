"""설정 항목별 "무엇이고 어떻게 쓰는가"를 담은 도움말 사전이다.

## 왜 config.yml 이 아니라 여기인가

``config.yml`` 의 줄 끝 주석은 한 줄짜리 요약이라 "이게 뭔지"는 알려 주지만 "얼마로 두어야
하는지"까지는 담기 어렵다. 그렇다고 주석을 문단째 늘리면 설정 파일이 읽기 어려워지고,
운영자가 업그레이드 때 손으로 옮겨야 하는 파일이 그만큼 무거워진다.

그래서 안내는 코드 쪽에 둔다. ``config.yml`` 은 값의 구조로 남기고, 설명은 새 버전을
설치하면 자동으로 따라오게 하려는 것이다. 대신 키가 사라지거나 이름이 바뀌면 안내가 조용히
붕 뜨므로, :mod:`tests.test_config_help` 가 모든 키가 실제 스키마에 있는지 확인한다.

## 무엇을 적는가

한 항목에 두 문장 안팎으로, **무엇을 정하는 값인지**와 **어떻게 정하는지**(무엇을 보고
올리고 내리는지, 무엇과 함께 움직여야 하는지)를 적는다. 기본값과 타입은 스키마에서 이미
나오므로 되풀이하지 않는다. 모든 항목을 채우지는 않는다 — 이름만으로 뜻이 분명한 항목
(``impala.port`` 같은)은 비워 두고, 잘못 잡으면 성능이나 정합성이 상하는 항목에 집중한다.
"""

from __future__ import annotations

# ─────────────────────────────────────────────────────────────────────────────
# 항목별 안내. 키는 config.properties 의 프로퍼티 키다.
# ─────────────────────────────────────────────────────────────────────────────
FIELD_HELP: dict[str, str] = {
    # ── 동시성 ────────────────────────────────────────────────────────────
    "coordinator.max_concurrent_jobs": (
        "동시에 RUNNING 일 수 있는 job 수다. 슬롯이 다 차면 다음 job 은 PENDING 으로 줄을 선다. "
        "여기서 정하는 것은 job 의 수일 뿐이고 실제 부하는 job 하나가 몇 개의 task 로 쪼개지는지에 "
        "달렸으므로, Impala 나 Greenplum 이 버거워하면 이 값보다 executor.max_concurrent_tasks 를 "
        "먼저 본다. 0 이하로 두면 admission 자체를 쓰지 않아 입구에서 아무것도 막지 않는다."
    ),
    "coordinator.max_pending_jobs": (
        "슬롯이 찼을 때 PENDING 으로 세워 둘 job 수다. 실행 슬롯과 이 값을 더한 만큼까지 받아 주고 "
        "그 위로는 429 로 거절한다. 잠깐 몰리는 요청을 흡수하는 완충 구간이라 넉넉해도 손해가 없지만, "
        "0 으로 두면 완충이 사라져 슬롯을 넘는 요청이 곧바로 429 가 된다(무제한이 아니다). "
        "클라이언트가 429 를 재시도하지 않는다면 넉넉히 잡는 편이 안전하다."
    ),
    "coordinator.max_dispatch_concurrency": (
        "coordinator 가 executor 로 task 를 동시에 몇 건까지 내보낼지 정한다. 플릿 전체 용량"
        "(executor 수 × executor.max_concurrent_tasks) 이상으로 두어야 executor 가 놀지 않는다. "
        "0 이면 디스패치가 영원히 멈추므로 1 아래로는 내릴 수 없다."
    ),
    "executor.max_concurrent_tasks": (
        "executor 한 대가 동시에 돌리는 task 수이며 실제 처리량과 부하를 가장 직접적으로 정하는 값이다. "
        "task 하나가 소스 커서 하나와 GP 연결 하나를 잡으므로, 올릴 때는 Impala 의 동시 쿼리 여력과 "
        "Greenplum 의 max_connections 를 함께 본다. 소스나 대상이 아니라 executor 자신의 CPU·메모리가 "
        "포화라면 이 값을 내리는 대신 executor 대수를 늘리는 편이 낫다. 0 은 무제한이라 운영에서는 권하지 않는다."
    ),
    "greenplum.pool_max": (
        "executor 한 대가 Greenplum 에 동시에 여는 연결 수의 상한이다. 0 으로 두면 "
        "executor.max_concurrent_tasks 를 따라가므로(동시 task 당 1 연결) 대개 0 이 정답이다. "
        "task 수보다 작게 잡으면 task 가 매번 연결을 기다려 처리량만 깎이고, GP 의 max_connections 를 "
        "지켜야 할 때만 의도적으로 낮춘다. 전체 연결 수는 executor 대수를 곱한 값이라는 점을 잊지 않는다."
    ),
    "copy.batch_size": (
        "COPY 로 한 번에 넘기는 행 수다. 키우면 왕복이 줄어 빨라지지만 그만큼 메모리를 더 쓴다. "
        "행이 넓거나(컬럼이 많거나 큰 문자열) executor 메모리가 빠듯하면 줄이고, 좁은 행이라면 "
        "키워도 무리가 없다. copy.queue_size 와 곱한 값이 task 하나가 들고 있는 최대 행 수다."
    ),
    "copy.queue_size": (
        "읽기와 쓰기를 겹쳐 돌릴 때 사이에 쌓아 두는 배치 개수다(copy.pipeline=true 일 때만 뜻이 있다). "
        "소스와 대상의 속도 차를 흡수하는 완충이라 2~3 이면 대개 충분하고, 더 키워도 처리량보다 "
        "메모리만 늘기 쉽다."
    ),
    "copy.pipeline": (
        "소스 읽기와 GP 쓰기를 별도 스레드로 겹쳐 돌린다. 둘 중 한쪽이 느려 다른 쪽이 기다리는 "
        "상황을 줄여 주므로 평소에는 켜 둔다. 메모리를 아껴야 하거나 문제를 좁혀 볼 때만 끈다."
    ),
    "stage.max_files_per_host": (
        "local_stage 에서 호스트 하나가 만들 CSV 파일 수의 상한이다. 0 이면 그 호스트의 primary "
        "세그먼트 수를 그대로 쓰는데, 세그먼트가 파일 하나씩 맡아 읽는 구조라 대개 이 값이 가장 좋다. "
        "세그먼트보다 파일이 많으면 일부 세그먼트가 두 번 일하게 되어 오히려 늦어진다."
    ),

    # ── 클러스터 구성 ─────────────────────────────────────────────────────
    "coordinator.executors": (
        "task 를 내보낼 executor 의 베이스 URL 목록이며 쉼표로 잇는다. 여기 적힌 순서와 개수가 "
        "분할 수와 플릿 용량의 기준이 된다. local_stage 를 쓴다면 각 URL 의 호스트가 실제 GP 세그먼트 "
        "호스트와 같아야 하고, HA 로 self-report 를 켠다면 executor.advertise_url 과 문자열이 정확히 같아야 "
        "부하 뷰가 한 executor 를 두 대로 세지 않는다."
    ),
    "coordinator.executor_mode": (
        "remote 는 HTTP 로 executor 에 디스패치하는 정상 운영 모드다. local 은 별도 executor 없이 "
        "coordinator 안에서 직접 실행하므로 개발이나 단일 노드 점검에 쓰고, 이때 coordinator.executors 는 보지 않는다."
    ),
    "coordinator.executor_select": (
        "task 를 보낼 executor 를 고르는 방식이다. round_robin 은 단순히 돌아가며 주므로 executor 성능이 "
        "고르고 task 길이가 비슷할 때 무난하다. least_loaded 는 가장 한가한 곳을 고르지만 여러 coordinator 가 "
        "같은 판단을 동시에 내려 한쪽으로 몰릴 수 있어, 멀티 coordinator 에서는 두 곳만 비교해 덜 바쁜 쪽을 "
        "고르는 p2c 를 권한다."
    ),
    "executor.advertise_url": (
        "self-report 로 상태를 공유 DB 에 적을 때 자신을 가리키는 URL 이다. coordinator.executors 의 "
        "해당 항목과 글자까지 같아야 하며, 다르면 같은 executor 가 부하 뷰에서 둘로 갈라져 배분이 어긋난다."
    ),
    "executor.gp_hostname": (
        "local_stage 에서 이 executor 가 함께 올라가 있는 GP 세그먼트 호스트명이며 "
        "gp_segment_configuration.hostname 과 정확히 같아야 한다. file:// 외부테이블 주소를 이 이름으로 "
        "조립하므로 틀리면 GP 가 CSV 를 찾지 못한다. s3_stage 만 쓴다면 비워 둔다."
    ),

    # ── 저장소와 이력 ─────────────────────────────────────────────────────
    "store.backend": (
        "job 상태를 어디에 두는지 정한다. memory 는 프로세스가 죽으면 진행 중이던 job 도 함께 사라지므로 "
        "개발이나 단발성 이관에 쓴다. file 은 단일 노드에서 재기동 후에도 상태를 이어 가고, postgres 는 "
        "여러 coordinator 가 상태를 공유해야 할 때 쓴다(history.db_dsn 이 함께 필요하다)."
    ),
    "store.path": (
        "backend=file 일 때 상태를 적어 둘 파일 경로다. 비우면 로그 디렉터리 옆에 만든다. "
        "재기동으로 살아남아야 하는 값이므로 tmpfs 처럼 날아가는 위치는 피한다."
    ),
    "history.db_dsn": (
        "job·task 실행 이력을 쌓을 PostgreSQL DSN 이다. 비우면 이력을 남기지 않아 대시보드의 "
        "지난 실행 화면이 빈다. task 이력은 executor 가 직접 적으므로 **executor 호스트에서도** 이 DB 에 "
        "닿아야 한다. store.backend=postgres 를 쓸 때는 반드시 채운다."
    ),
    "db.schema": (
        "jobs·job_history·task_history 같은 메타 테이블을 담을 스키마다. 바꾸면 앱 설정만이 아니라 "
        "config/postgresql.sql 과 config/warehousepg.sql 의 DDL 도 함께 고쳐야 둘이 같은 곳을 가리킨다."
    ),

    # ── 장애 대응 ─────────────────────────────────────────────────────────
    "coordinator.task_timeout_s": (
        "task 하나의 HTTP 응답을 기다리는 최대 시간이다. 큰 파티션을 읽는 이관은 몇십 분이 걸리기도 하므로 "
        "가장 오래 걸리는 task 의 실제 소요보다 넉넉히 잡는다. 짧으면 정상 동작 중인 task 를 실패로 처리한다."
    ),
    "coordinator.task_connect_timeout_s": (
        "executor 에 접속만 시도하는 시간이다. 죽은 executor 를 빨리 알아채기 위한 값이라 짧게 둔다"
        "(수 초). 전체 실행 시간과는 무관하므로 task_timeout_s 와 헷갈리지 않는다."
    ),
    "coordinator.task_max_retries": (
        "연결에 실패했을 때 같은 executor 로 다시 시도할 횟수이며 시도마다 대기가 배로 늘어난다. "
        "재시도가 끝나도 안 되면 task_failover 설정에 따라 다른 executor 로 넘어간다."
    ),
    "coordinator.task_failover": (
        "재시도를 다 쓴 task 를 다른 executor 로 옮겨 실행할지 정한다. 켜 두면 executor 한 대가 죽어도 "
        "job 이 살아남는다. 다만 local_stage 는 executor 와 GP 세그먼트가 짝지어 있어 옮겨 가면 짝이 "
        "깨지므로 그 모드에서는 신중히 쓴다."
    ),
    "coordinator.orphan_reconcile_interval_s": (
        "죽은 coordinator 가 들고 있던 job 을 정리하는 주기다. 멀티 coordinator 에서만 뜻이 있고 "
        "0 이면 정리하지 않아 그런 job 이 RUNNING 인 채로 남는다."
    ),

    # ── 쿼리와 템플릿 ─────────────────────────────────────────────────────
    "query.sql_dialect": (
        "SELECT 를 파싱할 때 쓰는 SQL 방언(dialect)이다. 소스가 Impala 면 hive 로 두면 대개 맞고, "
        "요청마다 따로 지정할 수도 있다. 파싱에만 쓰이므로 실제로 어느 엔진에 던질지는 datasource 가 정한다."
    ),
    "template.enabled": (
        "template_id 로 서버 템플릿을 렌더해 SQL 을 만드는 기능을 켠다. 끄면 클라이언트가 보낸 "
        "SQL 전문을 그대로 쓰는 예전 방식만 남으므로, 템플릿을 쓰던 요청은 그때부터 실패한다."
    ),
    "template.validate_ddl_single_stmt": (
        "렌더된 DDL 과 INSERT 가 각각 한 문장인지 확인해, 파라미터를 타고 여러 문장이 끼어드는 것을 막는다. "
        "안전장치이므로 켜 두고, 한 조각에 여러 문장을 넣어야 하는 템플릿을 쓸 때만 끈다."
    ),
    "template.dir": (
        "쿼리 템플릿을 찾을 루트 디렉터리이며 그 아래 <template_id>/manifest.yml 구조를 기대한다. "
        "install.sh 의 rsync 에서 빠져 있어 업그레이드로 덮이지 않으므로, 새 버전이 예제를 추가했다면 손으로 옮긴다."
    ),
    "template.auto_reload": (
        "템플릿 파일을 고칠 때마다 다시 읽는다. 개발 편의용이라 운영에서는 꺼 두어야 매 요청마다 "
        "디스크를 확인하지 않는다."
    ),
    "template.func_modules": (
        "템플릿에서 쓸 사용자 정의 필터·함수를 담은 모듈 경로 목록이다. customs/ 아래 코드를 여기 등록하면 "
        "Jinja2 렌더에서 바로 부를 수 있다."
    ),

    # ── 로깅 ──────────────────────────────────────────────────────────────
    "log.level": (
        "메인 로그에 남길 최소 레벨이다. 운영은 INFO 로 두고, DEBUG 는 HTTP 요청·응답 본문까지 "
        "쏟아지므로 문제를 좇는 동안만 잠깐 쓴다."
    ),
    "log.sql.enabled": (
        "소스와 대상에 실제로 던진 SQL 을 모두 남긴다. 무엇을 읽어 무엇을 적재했는지가 사고 추적의 "
        "출발점이라 로그 레벨과 무관하게 INFO 로 항상 기록하며, 특별한 사정이 없으면 켜 둔다."
    ),
    "log.sql.max_length": (
        "SQL 한 건을 몇 글자까지 남길지 정한다. IN 목록이 긴 쿼리는 로그를 크게 부풀리므로 상한을 두되, "
        "잘린 경우에는 전문이 아니라는 표시가 함께 남는다."
    ),
    "log.sql.params": (
        "바인드 파라미터(파티션 값 등)를 SQL 과 함께 남긴다. 어느 값 범위를 처리했는지 알려면 켜 두는 편이 "
        "좋고, 값 자체가 민감하다면 끈다."
    ),
    "log.http.enabled": (
        "HTTP 요청과 응답을 남긴다. 로그 레벨이 DEBUG 일 때만 실제로 기록되므로, 이 값은 'DEBUG 로 내려도 "
        "HTTP 만은 남기지 않겠다'는 뜻의 스위치에 가깝다."
    ),
    "log.http.bodies": (
        "요청·응답 본문까지 남길지 정한다. 본문에는 SQL 전문이나 자격증명이 담길 수 있어 마스킹을 거치지만, "
        "양이 많으므로 필요할 때만 켠다."
    ),

    # ── 소스와 대상 접속 ──────────────────────────────────────────────────
    "impala.host": (
        "이관 원본 Impala 의 호스트다. 이 값과 greenplum.dsn 이 모두 채워져야 실제 백엔드로 동작하고, "
        "하나라도 비면 아무것도 읽거나 쓰지 않는 MockBackend 로 뜬다(개발용). 운영에서 데이터가 "
        "안 들어온다면 여기부터 확인한다."
    ),
    "impala.query_options": (
        "모든 소스 쿼리 앞에 SET 으로 붙일 Impala 옵션이며 MEM_LIMIT=2g,REQUEST_POOL=etl 형태로 적는다. "
        "이관이 Impala 의 다른 작업을 밀어내지 않도록 전용 풀과 메모리 상한을 지정할 때 쓴다. "
        "요청에 같은 옵션이 있으면 그쪽이 이긴다."
    ),
    "greenplum.dsn": (
        "적재 대상 Greenplum 의 접속 문자열이다(postgresql://user:pw@host:port/db). 대상은 TLS 나 "
        "별도 인증 없이 이 DSN 하나로 붙는다. 비어 있으면 MockBackend 로 떨어져 실제 적재가 일어나지 않는다."
    ),

    # ── 스테이징 ──────────────────────────────────────────────────────────
    "stage.local_dir": (
        "local_stage 에서 CSV 를 떨어뜨릴 로컬 경로이며 모든 세그먼트 호스트가 같은 경로를 써야 한다"
        "(파일은 호스트마다 자기 몫만 다르다). job 별 하위 디렉터리로 나뉘므로 여러 job 이 섞이지 않는다. "
        "가장 큰 task 결과를 담을 여유가 있는 디스크를 고른다."
    ),
    "stage.csv_delimiter": (
        "CSV 컬럼 구분자다. executor 가 쓸 때와 GP 외부테이블이 읽을 때가 같은 값을 쓰므로 한쪽만 바꾸면 "
        "적재가 어긋난다. 데이터에 나타나지 않을 문자를 골라야 하며 기본값 backtick 은 그래서 쓴다."
    ),
    "stage.cleanup": (
        "적재에 성공한 뒤 CSV 와 외부테이블을 지운다. 켜 두는 것이 정상이고, 무엇이 적재됐는지 "
        "직접 확인해야 할 때만 잠깐 끈다(끄면 디스크가 계속 찬다)."
    ),
    "stage.validate_hosts": (
        "Phase 2 에 들어가기 전에 file:// 이 가리키는 호스트가 gp_segment_configuration 에 실제로 있는지 "
        "확인한다. 틀린 호스트명은 적재 도중에야 드러나므로 미리 잡는 편이 낫다."
    ),
    "s3.bucket": (
        "s3_stage 에서 CSV 를 올릴 버킷이다. 비우면 s3_stage 를 쓸 수 없을 뿐 다른 exec_mode 에는 영향이 없다. "
        "executor 는 업로드 권한이, GP 쪽 PXF 는 읽기 권한이 각각 필요하다."
    ),
    "s3.external_schema": (
        "Phase 2 에서 만드는 외부테이블을 담을 스키마다(예: dwtemp). 비우면 search_path 를 따르는데, "
        "스키마를 지정하면 임시 객체가 한곳에 모여 관리하기 쉽다. 다만 그 스키마는 미리 만들어져 있어야 한다."
    ),
    "s3.pxf_server": (
        "GP 가 S3 를 읽을 때 쓸 PXF SERVER 이름이며 자격증명은 $PXF_BASE/servers/<이름>/s3-site.xml 에 둔다. "
        "executor 의 업로드 자격증명(s3.access_key)과는 별개라, 한쪽만 맞으면 올라가긴 해도 읽지 못한다."
    ),
    "s3.delete_on_cleanup": (
        "적재가 끝난 뒤 S3 객체를 지운다. 켜 두지 않으면 버킷에 중간 산출물이 계속 쌓이므로, "
        "보관이 필요하다면 수명주기 정책을 따로 거는 편이 낫다."
    ),

    # ── 적재 동작의 세부 ──────────────────────────────────────────────────
    "coordinator.stage_unique_staging": (
        "stage_insert 에서 staging 테이블 이름에 task_id 를 붙이고 task 가 끝나면 지운다. "
        "GP 연결을 풀에서 재사용하는 구조라 이름이 같으면 앞 task 의 TEMP 테이블이 남아 "
        "'already exists' 로 부딪히므로 켜 두는 것이 정상이다."
    ),
    "copy.preflight": (
        "COPY 를 시작하기 전에 SELECT 의 컬럼이 대상 테이블에 실제로 있는지 확인한다. 켜 두면 "
        "컬럼이 어긋난 요청이 데이터를 반쯤 밀어 넣기 전에 실패하므로, 확인 쿼리 한 번 값은 한다."
    ),
    "copy.format": (
        "text 는 값을 문자열로 바꿔 보내고 binary 는 이진 표현으로 보내 인코딩 CPU 를 아낀다. "
        "다만 binary 는 타입을 카탈로그에서 해석해야 하고 실패하면 text 로 되돌아가므로, "
        "쓰기가 CPU 때문에 병목일 때만 시험 삼아 켠다."
    ),
    "stage.impala_convert_types": (
        "끄면(false) timestamp·date·decimal 을 Impala 가 보낸 문자열 그대로 받아 CSV 에 쓴다. "
        "어차피 CSV 로 나갈 값을 파이썬 객체로 바꿨다가 다시 문자열로 만드는 낭비가 사라지므로 "
        "export 경로에서는 꺼 두는 편이 빠르다. 값의 표기가 달라 보이면 그때 켜서 비교해 본다."
    ),
    "executor.shutdown_drain_timeout_s": (
        "SIGTERM 을 받은 뒤 진행 중인 task 가 끝나기를 기다리는 시간이다. 재기동할 때 이 시간 안에 "
        "끝난 task 는 살아남고 넘긴 task 는 잘리므로, 평소 task 하나가 걸리는 시간보다 넉넉히 잡되 "
        "systemd 의 TimeoutStopSec 보다는 짧아야 뜻이 있다."
    ),
    "coordinator.poll_interval_s": (
        "디스패치한 task 의 상태를 얼마나 자주 물어볼지 정한다. 짧으면 진행률이 매끄럽지만 task 가 "
        "많을수록 executor 를 그만큼 자주 두드리고, 길면 종료 감지가 그만큼 늦어진다."
    ),

    # ── 멀티 coordinator(HA) ──────────────────────────────────────────────
    "coordinator.id": (
        "여러 coordinator 가 공유 저장소를 함께 쓸 때 서로를 가려내는 이름이다. 비우면 기동할 때마다 "
        "임의로 지어지므로, 어느 coordinator 가 어떤 job 을 맡았는지 이력에서 알아보려면 호스트마다 "
        "고정된 이름을 준다."
    ),
    "coordinator.executor_health_source": (
        "executor 의 부하를 무엇으로 판단할지 정한다. auto 는 coordinator 가 여럿이면 executor 의 "
        "self-report 를, 한 대뿐이면 자체 헬스체크를 쓰도록 알아서 고르므로 대개 auto 로 둔다. "
        "self_report 를 쓰려면 executor.self_report 도 함께 켜야 한다."
    ),
    "coordinator.executor_reservation": (
        "디스패치하는 동안 executor 자리를 미리 잡아 둔다. 여러 coordinator 가 같은 순간에 '가장 "
        "한가한 곳'을 똑같이 골라 한쪽으로 몰리는 일을 막지만, 공유 저장소에 예약을 쓰고 지우는 "
        "비용이 붙는다. 배분이 실제로 쏠릴 때만 켠다."
    ),
    "coordinator.reservation_ttl_s": (
        "예약이 저절로 풀리는 시간이다. coordinator 가 예약만 해 두고 죽으면 그 자리가 영영 잠기므로, "
        "디스패치가 끝나기에 충분하면서 지나치게 길지 않은 값으로 둔다."
    ),

    # ── 접속과 자격증명 ───────────────────────────────────────────────────
    "impala.auth_mechanism": (
        "소스 Impala 의 인증 방식이며 기본은 LDAP 이다. 여기서 정하는 TLS 와 인증은 **소스에만** "
        "적용되고 적재 대상 Greenplum 은 greenplum.dsn 하나로 붙는다는 점을 헷갈리지 않는다."
    ),
    "impala.use_ssl": (
        "소스 접속에 TLS 를 쓴다. 사설 인증서라면 impala.ca_cert 에 CA 를 함께 지정해야 검증을 "
        "통과한다."
    ),
    "s3.access_key": (
        "executor 가 CSV 를 **올릴 때** 쓰는 자격증명이며, 비우면 boto3 의 기본 순서(인스턴스 역할 등)를 "
        "따른다. GP 가 **읽을 때** 쓰는 자격증명은 s3.pxf_server 쪽에 따로 있으므로, 한쪽만 맞으면 "
        "업로드는 되는데 적재가 안 되는 상태가 된다."
    ),
    "s3.prefix": (
        "객체 키 앞에 붙는 경로이며 실제 키는 <prefix>/<job_id>/<task_id>.csv 가 된다. job 마다 "
        "디렉터리가 갈리므로 Phase 2 의 외부테이블은 그 job 의 파일만 가리킨다."
    ),

    # ── 모니터링 ──────────────────────────────────────────────────────────
    "executor.self_report": (
        "executor 가 자기 상태를 공유 DB 에 직접 적는다. coordinator 가 여러 대라 각자 헬스체크를 "
        "돌리는 것이 낭비이거나 서로 다른 판단을 내리는 상황을 막을 때 켠다. 단일 coordinator 라면 "
        "monitor 쪽 헬스체크만으로 충분하다."
    ),
    "monitor.enabled": (
        "coordinator 가 주기적으로 executor 의 헬스와 자원 사용량을 확인한다. 끄면 대시보드의 "
        "executor 상태가 갱신되지 않고 least_loaded·p2c 선택도 판단 근거를 잃는다."
    ),
    "monitor.db_dsn": (
        "수집한 메트릭을 쌓을 DB DSN 이다. 비우면 화면에만 보이고 기록은 남지 않으므로, 지난 부하를 "
        "돌아봐야 한다면 채운다."
    ),
    "dashboard.enabled": (
        "읽기 전용 웹 대시보드를 연다. 쓰기 동작이 없어 켜 두어도 안전하지만, 외부에 노출되는 포트라면 "
        "job 목록과 SQL 이 보인다는 점을 감안한다."
    ),
}

# 함께 보아야 뜻이 통하는 항목들이다. 도움말 화면 아래에 '함께 보기'로 붙는다.
RELATED: dict[str, list[str]] = {
    "coordinator.max_concurrent_jobs": ["coordinator.max_pending_jobs", "executor.max_concurrent_tasks"],
    "coordinator.max_pending_jobs": ["coordinator.max_concurrent_jobs"],
    "coordinator.max_dispatch_concurrency": ["coordinator.executors", "executor.max_concurrent_tasks"],
    "executor.max_concurrent_tasks": ["greenplum.pool_max", "coordinator.max_dispatch_concurrency"],
    "greenplum.pool_max": ["executor.max_concurrent_tasks"],
    "copy.batch_size": ["copy.queue_size", "copy.pipeline"],
    "copy.queue_size": ["copy.batch_size", "copy.pipeline"],
    "copy.pipeline": ["copy.queue_size"],
    "store.backend": ["history.db_dsn", "store.path", "executor.self_report"],
    "history.db_dsn": ["store.backend", "db.schema"],
    "executor.self_report": ["executor.advertise_url", "coordinator.executor_health_source"],
    "executor.advertise_url": ["coordinator.executors", "executor.self_report"],
    "executor.gp_hostname": ["stage.local_dir", "stage.validate_hosts"],
    "impala.host": ["greenplum.dsn"],
    "greenplum.dsn": ["impala.host", "greenplum.pool_max"],
    "stage.local_dir": ["executor.gp_hostname", "stage.max_files_per_host"],
    "stage.max_files_per_host": ["stage.local_dir"],
    "s3.bucket": ["s3.pxf_server", "s3.external_schema", "s3.delete_on_cleanup"],
    "s3.pxf_server": ["s3.bucket"],
    "log.sql.enabled": ["log.sql.max_length", "log.sql.params"],
    "log.http.enabled": ["log.level", "log.http.bodies"],
    "coordinator.executor_select": ["coordinator.executor_health_source", "monitor.enabled"],
    "coordinator.task_timeout_s": ["coordinator.task_connect_timeout_s"],
    "coordinator.task_connect_timeout_s": ["coordinator.task_max_retries", "coordinator.task_failover"],
    "coordinator.id": ["store.backend", "coordinator.executor_health_source"],
    "coordinator.executor_health_source": ["executor.self_report", "monitor.enabled"],
    "coordinator.executor_reservation": ["coordinator.reservation_ttl_s", "coordinator.executor_select"],
    "coordinator.reservation_ttl_s": ["coordinator.executor_reservation"],
    "coordinator.stage_unique_staging": ["greenplum.pool_max"],
    "copy.format": ["copy.preflight", "copy.batch_size"],
    "copy.preflight": ["copy.format"],
    "impala.auth_mechanism": ["impala.use_ssl", "impala.ca_cert", "greenplum.dsn"],
    "impala.use_ssl": ["impala.ca_cert", "impala.auth_mechanism"],
    "s3.access_key": ["s3.secret_key", "s3.pxf_server"],
    "s3.prefix": ["s3.bucket", "s3.delete_on_cleanup"],
    "stage.impala_convert_types": ["stage.csv_delimiter"],
    "template.enabled": ["template.dir", "template.func_modules"],
    "template.validate_ddl_single_stmt": ["template.enabled"],
}


def help_for(prop_key: str) -> str:
    """항목의 안내 문장을 돌려준다. 준비된 안내가 없으면 빈 문자열이다."""
    return FIELD_HELP.get(prop_key, "")


def related_to(prop_key: str) -> list[str]:
    """함께 보면 좋은 항목의 키 목록을 돌려준다."""
    return RELATED.get(prop_key, [])
