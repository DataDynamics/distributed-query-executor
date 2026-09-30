"""`POST /jobs` 부하 테스트 도구다(`bin/load-test`).

JMeter 의 Thread Group 처럼 가상 사용자(VU)를 ramp-up 으로 늘려 가며 coordinator 에 job 을
제출한다. `/jobs` 는 모든 exec_mode 에서 202 로 바로 돌아오고 실제 처리는 비동기로 진행되므로,
VU 는 제출한 뒤 ``GET /jobs/{id}/status`` 를 폴링해 종료 상태를 확인하고 나서야 다음 요청을
보낸다(closed model). 그래서 "초당 접수 건수"와 "초당 완료 건수"를 따로 잰다. 비동기 API 에서
접수 TPS 만 보면 서버가 실제로 처리하는 양을 크게 부풀려 보게 되기 때문이다.

테스트하는 동안 ``GET /cluster`` 를 주기적으로 불러 coordinator 와 모든 executor 의 CPU·메모리를
시계열로 모으고, 끝나면 처리량·지연과 함께 서버별 자원 요약을 한 화면에 낸다.

모듈 구성은 다음과 같다.

- :mod:`tools.load.scenario` 는 ramp-up 단계 계산과 요청 본문 치환을 맡는 순수 함수 모음이다.
- :mod:`tools.load.client` 는 coordinator HTTP 호출(제출·폴링·취소·클러스터 조회)을 감싼다.
- :mod:`tools.load.runner` 는 VU 를 만들고 회수하며 종료와 drain 을 조율한다.
- :mod:`tools.load.sampler` 는 서버 자원을 주기적으로 수집한다.
- :mod:`tools.load.stats` 는 지연 백분위와 초 단위 시계열을 집계한다.
- :mod:`tools.load.report` 는 콘솔 요약과 JSON·CSV 결과 파일을 만든다.
- :mod:`tools.load.cli` 는 명령행과 시나리오 YAML 을 합쳐 위 구성요소를 실행한다.
"""
