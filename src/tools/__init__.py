"""운영자용 CLI 도구 모음이다(`bin/gp-shell`·`bin/impala-shell`·`bin/s3-ops`).

coordinator·executor 서비스와 별개로, 사람이 터미널에서 직접 쓰는 도구들을 모아 둔다.
서비스가 하는 일(대량 이관)과 목적이 달라 패키지를 분리했다. 이관 중에 무엇이 들어갔는지
바로 확인하거나(SQL 셸) 스테이징 객체를 정리할 때(S3 조작) 쓴다.

- :mod:`tools.gp_query` 는 Greenplum 에, :mod:`tools.impala_query` 는 Impala 에 SQL 을
  실행한다. ``--interactive`` 를 주면 :mod:`tools.shell` 의 대화형 셸로 들어간다.
- :mod:`tools.s3_ops` 는 S3 객체를 올리고 내리고 지운다.
- :mod:`tools.appconfig` 가 접속 정보를 이 저장소의 `config.properties`/`config.yml` 에서
  읽어 오므로, 같은 값을 두 곳에 적을 일이 없다.

`bin/` 의 래퍼는 대화형 셸만 노출하지만 모듈 자체는 한 번 실행(batch)도 지원한다.
그 경우 `PYTHONPATH=src python -m tools.gp_query -q "SELECT 1"` 처럼 직접 부른다.
"""
