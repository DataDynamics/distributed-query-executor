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
그림으로 좇는다.

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
task 하나의 시간 분해(`task-timing.svg`), `s3_stage` 의 3단계(`s3-stage-phases.svg`), 그리고 이
디렉터리의 구성(`merge-map.svg`)이 있다.

클래스 그림은 `class-*.svg`, 시퀀스 그림은 `seq-*.svg` 이며 이 둘은 같은 이름의 PNG 도 함께 둔다.
PNG 는 가로 두 배 해상도로 뽑아 두었으므로 SVG 를 받지 않는 발표 자료나 사내 위키에 그대로 쓸 수
있다. 그림을 고칠 때는 SVG 를 고치고 PNG 를 다시 뽑아 둘을 함께 갱신한다.

## `docs/` 와의 관계

`docs/USER.md` 와 `docs/OPERATOR.md` 는 이 저장소 안의 서비스만 다루는 역할별 문서이고, DESIGN·
GUIDE·INTEGRATION·DEPLOY·PERFORMANCE 는 주제별 심화 문서다. 이 디렉터리의 두 문서는 거기에 터미널
도구 쪽 운영 지식까지 합쳐 **두 저장소를 함께 쓰는 환경**을 하나로 설명한 산출물이다. 겹치는 사실을
바꿀 때는 양쪽을 함께 고친다.
