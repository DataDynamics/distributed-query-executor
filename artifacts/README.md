# artifacts

두 저장소에 흩어져 있던 문서를 독자별로 하나씩 정리해 둔 디렉터리다. 여기 있는 두 문서는 각각
self-contained 문서라, 링크를 따라다니지 않고 하나만 읽어도 그 역할의 일을 끝낼 수 있게 필요한
내용을 모두 담았다.

![두 저장소를 독자별 문서 두 벌로 합쳤다](images/merge-map.svg)

[USER_GUIDE.md](USER_GUIDE.md) 는 이관 작업을 맡기고 결과를 확인하는 사람을 위한 것이다.
coordinator API 사용법과 `exec_mode` 선택, 템플릿과 날짜 fan-out, 오류 코드에 더해 터미널
도구(`gp-shell`·`impala-shell`·`s3-ops`) 사용법과 두 축을 함께 쓰는 확인 절차까지 한 벌로 담았다.

[OPERATOR_GUIDE.md](OPERATOR_GUIDE.md) 는 서비스를 설치하고 돌보는 사람을 위한 것이다. 설치와 설정,
일상 점검과 로그 읽기, 동시성과 용량 산정, 장애 추적, 멀티 coordinator 와 이력 DB, `s3_stage` 를
위한 S3 와 PXF 준비, 적재 대상의 분산키 선정, 도구를 여럿이 쓰게 만들 때의 자격증명과 크론, 그리고
업그레이드 절차를 다룬다.

[class-diagram.md](class-diagram.md) 와 [sequence-diagram.md](sequence-diagram.md) 는 코드를 여는
사람을 위한 것이다. 앞의 것은 기능별로 어느 파일에 무엇이 있는지를 일곱 장의 클래스 그림으로
보여 주고, 뒤의 것은 요청 하나가 들어와 끝날 때까지 누가 누구를 언제 부르는지를 여섯 장의 시퀀스
그림으로 좇는다. 두 문서 끝에는 같은 계열의 이관 도구인 DataDynamics/whpg-to-whpg 를 같은 방식으로
그린 부록이 붙어 있다. 클래스 두 장과 local · s3 두 모드의 시퀀스 두 장이다.

[sw-architecture.md](sw-architecture.md) 는 SW 의 구조와 그 적용을 다루는 SW 아키텍처 정의서다.
표준 양식의 목차를 이 시스템에 맞게 고쳐, SW 기술 구조를 API·제어와 실행·적재, CLI·운영 도구 세
유형으로 나눠 정의하고 그것이 실제 코드에 어떻게 적용됐는지와 연계 아키텍처까지 담았다.

[tech-architecture.md](tech-architecture.md) 는 어떤 인프라 위에 올리고 어떻게 운영하는지를 다루는
기술 아키텍처 정의서다. 소프트웨어 구성과 구성요소별 정의, 구성요소 매핑에 더해 백업·복구와 보안,
가용성 방안이 들어 있다. 하드웨어와 네트워크처럼 아직 정하지 않은 절은 표 골격만 두고 비워 두었다.

다섯 문서는 다른 문서에 옮겨 붙이기 좋도록 Word 판을 함께 둔다 — 두 가이드
([USER_GUIDE.docx](USER_GUIDE.docx), [OPERATOR_GUIDE.docx](OPERATOR_GUIDE.docx))와 세 정의서
([sw-architecture.docx](sw-architecture.docx), [tech-architecture.docx](tech-architecture.docx),
[data-architecture.docx](data-architecture.docx))다.
Word 기본 내장 스타일만 써서 꾸밈을 넣지 않았으므로, 붙여 넣는 문서의 서식을 그대로 따라간다.
원본은 어디까지나 `.md` 이고 Word 판은 그것을 옮긴 것이므로, 내용을 고칠 때는 `.md` 를 고친 뒤
다시 변환한다. `docs/` 의 사용자·운영자 문서도 같은 방식으로 만든 `USER.docx`·`OPERATOR.docx` 를
함께 둔다.

[data-architecture.md](data-architecture.md) 는 같은 시스템을 데이터 관점에서 본 데이터 아키텍처
정의서다. 데이터를 소유하지 않고 옮긴다는 전제에서 출발해 이관·메타·중간 세 갈래로 영역을 가르고,
식별자와 시각·CSV 형식·타입 처리 표준, 메타 데이터 모델, 데이터가 갈라지고 합쳐지는 흐름, 재실행
안전성과 분할의 정확성, 보안과 수명주기, 용량 산정까지 담았다.

[er-diagram.md](er-diagram.md) 는 이 시스템이 무엇을 기억하는지를 다룬다. 개념과 관계만 보는 논리
ER 한 장과, 실제 DDL 이 만드는 테이블을 그대로 보여 주는 물리 ER 두 장(PostgreSQL 판과 WarehousePG
판)이 들어 있다. [tables.md](tables.md) 는 그 물리 테이블 일곱 개를 컬럼 단위로 적은 명세서다.
컬럼마다 타입과 길이, NOT NULL, 키, 기본값, 설명, 예제값을 담았고 `config/postgresql.sql` 과
자동으로 대조해 맞췄다. 같은 내용을 테이블별 시트로 나눈 엑셀이 [tables.xlsx](tables.xlsx) 다.

## 무엇을 합쳤는가

**DataDynamics/distributed-query-executor** 에서는 coordinator·executor 서비스와 그 운영을 가져왔다.
요청 흐름과 실행 모드, 템플릿 엔진, 동시성 파라미터, 멀티 coordinator, 모니터링과 로그가 여기에
해당한다. **DataDynamics/impala-to-whpg** 에서는 사람이 터미널에서 직접 쓰는 도구와 그 운영을
가져왔다. 쿼리 실행과 CSV 출력, 대화형 셸, S3 객체 조작, 자격증명과 크론, 종료 코드, PXF 와 S3
외부테이블 구성, 분산키 선정이 여기에 해당한다.

두 번째 저장소의 도구는 이미 이 저장소의 `src/tools/` 로 옮겨져 `bin/gp-shell`·`bin/impala-shell`·
`bin/s3-ops` 로 제공되며, **별도 설정 파일 없이 `config/config.properties` 를 그대로 읽는다.** 그래서
여기 두 문서는 서비스와 도구를 하나의 설정 체계 위에서 함께 설명한다. 원본 저장소의 서술 중 이
저장소에 맞지 않는 부분, 이를테면 자체 `conf/config.yaml` 이나 `bin/gp-query`·`bin/impala-query`
래퍼, psycopg2 같은 것들은 이 저장소의 실제 구성인 설정 어댑터와 모듈 직접 호출, psycopg 3 에 맞춰
고쳐 실었다.

## 그림

문서에 들어가는 그림은 `images/` 아래에 SVG 로 두었다. 에어갭 환경을 전제로 하므로 외부 이미지
호스트를 참조하지 않고 저장소 안의 파일만 쓰며, 벡터라 확대해도 글자가 뭉개지지 않는다. 가이드 쪽에는
전체 구성(`architecture.svg`), 작업 상태 전이(`job-lifecycle.svg`), 실행 모드별 데이터 경로
(`exec-modes.svg`), 제출부터 확인까지의 흐름(`verify-loop.svg`), 과부하 방어 세 층(`admission.svg`),
task 하나의 시간 분해(`task-timing.svg`), `s3_stage` 의 3단계(`s3-stage-phases.svg`), 백업
구성도(`backup-topology.svg`), 연계 논리 모델(`integration-model.svg`), 데이터 세
갈래(`data-domains.svg`)와 그 수명주기(`data-lifecycle.svg`), 그리고 이 디렉터리의
구성(`merge-map.svg`)이 있다.

클래스 그림은 `class-*.svg`, 시퀀스 그림은 `seq-*.svg`, ER 그림은 `er-*.svg` 이며 이 셋은 같은
이름의 PNG 도 함께 둔다. 여기에 문서의 Word 판이 쓰는 그림
(`architecture` · `admission` · `exec-modes` · `s3-stage-phases` · `backup-topology` ·
`integration-model` · `data-domains` · `data-lifecycle` · `job-lifecycle` · `verify-loop` ·
`task-timing`)도 PNG 를 함께 둔다. Word 가 SVG 를 그리지 못하기 때문이다.
PNG 는 가로 두 배 해상도로 뽑아 두었으므로 SVG 를 받지 않는 발표 자료나 사내 위키에 그대로 쓸 수
있다. 그림을 고칠 때는 SVG 를 고치고 PNG 를 다시 뽑아 둘을 함께 갱신한다.

## `docs/` 와의 관계

`docs/USER.md` 와 `docs/OPERATOR.md` 는 이 저장소 안의 서비스만 다루는 역할별 문서이고, DESIGN·
GUIDE·INTEGRATION·DEPLOY·PERFORMANCE 는 주제별 심화 문서다. 이 디렉터리의 두 문서는 거기에 터미널
도구 쪽 운영 지식까지 합쳐 **두 저장소를 함께 쓰는 환경**을 하나로 설명한 산출물이다. 겹치는 사실을
바꿀 때는 양쪽을 함께 고친다.
