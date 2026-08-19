# artifacts

두 저장소에 흩어져 있던 문서를 독자별로 하나씩 정리해 둔 디렉터리다. 여기 있는 두 문서는 각각
자립형이라, 링크를 따라다니지 않고 하나만 읽어도 그 역할의 일을 끝낼 수 있게 필요한 내용을 모두
담았다.

| 문서 | 누구를 위한 것인가 |
|---|---|
| [USER_GUIDE.md](USER_GUIDE.md) | 이관 작업을 맡기고 결과를 확인하는 사람 — coordinator API 사용법, `exec_mode` 선택, 템플릿과 날짜 fan-out, 오류 코드, 그리고 터미널 도구(`gp-shell`·`impala-shell`·`s3-ops`) 사용법 |
| [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md) | 서비스를 설치하고 돌보는 사람 — 설치와 설정, 일상 점검과 로그, 동시성·용량 산정, 장애 추적, HA 와 이력 DB, S3/PXF 준비, 분산키, 도구를 여럿이 쓰게 만들기, 업그레이드 |

## 무엇을 합쳤는가

- **DataDynamics/distributed-query-executor** — coordinator·executor 서비스와 그 운영. 요청 흐름,
  실행 모드, 템플릿 엔진, 동시성 손잡이, 멀티 coordinator, 모니터링과 로그.
- **DataDynamics/impala-to-whpg** — 사람이 터미널에서 직접 쓰는 도구와 그 운영. 쿼리 실행과 CSV
  출력, 대화형 셸, S3 객체 조작, 자격증명과 크론, 종료 코드, PXF·S3 외부테이블 구성, 분산키 선정.

두 번째 저장소의 도구는 이미 이 저장소의 `src/tools/` 로 옮겨져 `bin/gp-shell`·`bin/impala-shell`·
`bin/s3-ops` 로 제공되며, **별도 설정 파일 없이 `config/config.properties` 를 그대로 읽는다.** 그래서
여기 두 문서는 서비스와 도구를 하나의 설정 체계 위에서 함께 설명한다. 원본 저장소의 서술 중 이
저장소에 맞지 않는 부분(자체 `conf/config.yaml`, `bin/gp-query`·`bin/impala-query` 래퍼, psycopg2)은
이 저장소의 실제 구성(설정 어댑터, 모듈 직접 호출, psycopg 3)에 맞춰 고쳐 실었다.

## `docs/` 와의 관계

`docs/USER.md` 와 `docs/OPERATOR.md` 는 이 저장소 안의 서비스만 다루는 역할별 문서이고, DESIGN·
GUIDE·INTEGRATION·DEPLOY·PERFORMANCE 는 주제별 심화 문서다. 이 디렉터리의 두 문서는 거기에 터미널
도구 쪽 운영 지식까지 합쳐 **두 저장소를 함께 쓰는 환경**을 하나로 설명한 산출물이다. 겹치는 사실을
바꿀 때는 양쪽을 함께 고친다.
