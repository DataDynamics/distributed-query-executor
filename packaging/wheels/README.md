# 에어갭용 Python 휠 번들 (cp39 / RHEL 9.2)

인터넷이 차단된 환경에서 `pip install --no-index` 로 설치하기 위해 미리 받아 둔
파이썬 패키지를 **유형별**로 나눠 둔다. 모두 Python 3.9(cp39) + manylinux(glibc ≤ 2.28,
RHEL 9.2 의 2.34 호환) 기준이다.

| 디렉터리 | 내용 | 용도 |
|---|---|---|
| `coordinator/` | coordinator 런타임 의존성(`requirements.txt`) + pip/setuptools/wheel 부트스트랩 | 기본 설치(모든 구성의 베이스) |
| `executor/` | executor 추가 드라이버(impyla·thrift·SASL) + `gssapi` **sdist** + Cython | 실 Impala/Greenplum 연동 |
| `dev/` | pytest·pytest-asyncio 등 테스트 의존성 | 타깃에서 테스트 실행 시 |

- `coordinator/` 가 베이스이고, `executor/`·`dev/` 는 그와 **중복되지 않는 추가분**만 담는다.
  따라서 executor/dev 설치 시 `coordinator/` 도 함께 `--find-links` 로 지정한다.
- `gssapi` 는 manylinux 휠이 없어 **sdist** 로 넣었다. 타깃에서 빌드되며
  `gcc`, `krb5-devel`, `cyrus-sasl-devel`, `python3-devel`(RHEL DVD repo) 이 필요하다.

## 오프라인 설치

```bash
# coordinator 만
sudo WHEELHOUSE=packaging/wheels/coordinator ./deploy/install.sh

# executor 포함(콜론으로 여러 디렉터리 지정 → 다중 --find-links)
sudo WHEELHOUSE=packaging/wheels/coordinator:packaging/wheels/executor \
     INSTALL_EXECUTOR=1 ./deploy/install.sh

# 수동 설치 예시
pip install --no-index \
  --find-links packaging/wheels/coordinator \
  --find-links packaging/wheels/executor \
  -r requirements-executor.txt
```

> 사내 Nexus PyPI 프록시가 있으면 이 번들 대신 `pip install -i <nexus>/simple` 도 가능하다.
> 이 번들은 그런 프록시조차 없는 완전 오프라인 설치용 폴백이다.
