# 에어갭용 Python 휠 번들 (cp39 / RHEL 9.2)

인터넷이 막힌 서버에서는 `pip` 이 패키지를 내려받을 곳이 없습니다. 평소에는 `pip install`
한 줄이면 인터넷 어딘가의 저장소(PyPI)에서 필요한 파일을 알아서 받아 오지만, 외부와 완전히
단절된 망(이런 환경을 흔히 "에어갭"이라고 부릅니다)에서는 그 통로가 없습니다. 그래서 미리
받아 둔 설치 파일을 한곳에 모아 두고, `pip` 에게 "인터넷 대신 이 폴더에서 찾아라"라고 알려
주는 방식으로 설치합니다. 이 디렉터리가 바로 그 "미리 받아 둔 설치 파일"을 모아 둔 곳입니다.

여기서 말하는 설치 파일은 대부분 **휠(wheel)** 입니다. 휠은 파이썬 패키지를 미리 빌드해
하나의 파일로 묶어 둔 형식으로, 받아서 그대로 풀어 넣기만 하면 설치가 끝나기 때문에 설치가
빠르고 별도의 컴파일이 필요 없습니다. 이 번들에 담긴 휠은 모두 **Python 3.9** 용으로
만들어졌습니다. 휠 파일 이름에 붙는 `cp39` 라는 꼬리표가 바로 "CPython 3.9 전용"이라는
뜻입니다. 또한 리눅스 배포판마다 제각각인 시스템 라이브러리 차이를 흡수하기 위해
**manylinux** 라는 표준을 따릅니다. manylinux 는 "어지간한 리눅스라면 어디서나 돈다"는
의미로, 시스템의 핵심 C 라이브러리인 glibc 의 버전이 일정 수준 이하(여기서는 glibc ≤ 2.28)
에서 빌드되어 있으면 그보다 높은 버전에서도 호환됩니다. 우리가 설치할 RHEL 9.2 는 glibc
2.34 를 쓰는데, 이는 2.28 기준으로 만든 휠과 호환되므로 문제없이 동작합니다.

이 모든 패키지를 한 폴더에 그냥 쏟아 두지 않고 **유형별로 세 디렉터리에 나눠** 두었습니다.
어떤 디렉터리에 무엇이 들어 있고 언제 쓰는지는 아래 표에 정리해 두었습니다.

| 디렉터리 | 내용 | 용도 |
|---|---|---|
| `coordinator/` | coordinator 런타임 의존성(`requirements.txt`, Jinja2 템플릿 엔진 포함) + pip/setuptools/wheel 부트스트랩 | 기본 설치(모든 구성의 베이스) |
| `executor/` | executor 추가 드라이버(impyla·thrift·SASL·**trino**) + Cython | 실 Impala/Trino/Greenplum 연동 |
| `dev/` | pytest·pytest-asyncio 등 테스트 의존성 | 타깃에서 테스트 실행 시 |

이렇게 나눈 데에는 이유가 있습니다. 세 디렉터리는 서로 독립적인 묶음이 아니라, `coordinator/`
를 토대로 삼고 그 위에 필요한 만큼 덧붙이는 구조입니다. 풀어서 설명하면 다음과 같습니다.

- `coordinator/` 가 모든 설치의 토대(베이스)입니다. coordinator 를 돌리는 데 필요한
  런타임 의존성과 함께, 설치 작업 자체에 쓰이는 `pip`·`setuptools`·`wheel` 같은
  부트스트랩 도구까지 들어 있습니다. 그래서 `executor/` 와 `dev/` 는 같은 패키지를 또
  담지 않고, `coordinator/` 에 없는 **추가분만** 담습니다. 이 때문에 executor 나 dev 를
  설치할 때는 해당 디렉터리뿐 아니라 `coordinator/` 도 함께 `--find-links` 로 지정해서,
  pip 이 두 폴더를 모두 뒤져 가며 필요한 휠을 찾도록 해야 합니다. 여기서 `--find-links`
  란 "이 폴더(또는 위치)에서 설치 파일을 찾아라"라고 pip 에게 일러 주는 옵션입니다.

## 오프라인 설치

이제 실제로 설치하는 방법입니다. 가장 간단한 길은 저장소에 들어 있는 `packaging/install.sh`
스크립트를 쓰는 것입니다. 이 스크립트에 `WHEELHOUSE` 라는 환경변수로 "휠이 모여 있는
폴더"의 위치를 알려 주면, 스크립트가 알아서 `--no-index`(인터넷 저장소를 쳐다보지 말라는
뜻)와 `--find-links` 를 붙여 설치를 진행합니다. 휠을 모아 둔 이 폴더를 흔히
**WHEELHOUSE**(휠 창고)라고 부릅니다.

coordinator 만 설치할 때는 `WHEELHOUSE` 에 `coordinator` 디렉터리 하나만 지정하면 됩니다.

```bash
# coordinator 만
sudo WHEELHOUSE=packaging/wheels/coordinator ./packaging/install.sh
```

executor 까지 설치하려면 앞서 설명한 대로 `coordinator/` 와 `executor/` 두 폴더를 모두
넘겨야 합니다. 여러 폴더는 콜론(`:`)으로 이어 붙여 지정하며, 스크립트는 이를 여러 개의
`--find-links` 로 풀어 줍니다.

```bash
# executor 포함(콜론으로 여러 디렉터리 지정 → 다중 --find-links)
sudo WHEELHOUSE=packaging/wheels/coordinator:packaging/wheels/executor \
     INSTALL_EXECUTOR=1 ./packaging/install.sh
```

스크립트를 거치지 않고 `pip` 을 직접 호출하고 싶다면 다음처럼 손으로 옵션을 붙여도 됩니다.
`--no-index` 로 인터넷을 끊고, `--find-links` 를 폴더마다 하나씩 붙여 pip 이 그 폴더들에서만
휠을 찾도록 한 다음, `-r` 로 설치할 패키지 목록 파일을 지정하는 방식입니다.

```bash
# 수동 설치 예시
pip install --no-index \
  --find-links packaging/wheels/coordinator \
  --find-links packaging/wheels/executor \
  -r requirements-executor.txt
```

마지막으로, 완전한 에어갭이 아니라 사내에 패키지 저장소를 대신해 주는 중계 서버가 있는
경우도 있습니다. 대표적인 것이 **Nexus PyPI 프록시** 인데, 이는 외부 PyPI 를 회사 내부에서
대신 받아다 캐시해 주는 사내 저장소를 말합니다. 그런 프록시가 갖춰져 있다면 굳이 이 번들을
쓰지 않고 인터넷 저장소를 가리키듯 그 주소를 지정해 설치할 수 있습니다. 이 휠 번들은 그런
프록시조차 없는, 진짜로 외부와 완전히 끊긴 환경을 위한 최후의 대비책(폴백)입니다.

> 사내 Nexus PyPI 프록시가 있으면 이 번들 대신 `pip install -i <nexus>/simple` 도 가능하다.
> 이 번들은 그런 프록시조차 없는 완전 오프라인 설치용 폴백이다.
