# systemd 배포 가이드 (RHEL 9.2)

coordinator 1개와 executor 다수를 systemd 서비스로 운영하기 위한 구성이다.

## 구성 파일

| 파일 | 설명 |
|---|---|
| `systemd/query-coordinator.service` | coordinator 서비스 유닛 |
| `systemd/query-executor@.service` | executor **템플릿** 유닛(인스턴스 이름 = 포트) |
| `systemd/coordinator.env.example` | coordinator 환경설정 예시 |
| `systemd/executor.env.example` | executor 환경설정 예시 |
| `install.sh` | 사용자/디렉터리/venv/유닛을 한 번에 구성하는 설치 스크립트 |

- **executor는 템플릿 유닛**이라 포트별로 여러 인스턴스를 띄운다: `query-executor@8001`, `query-executor@8002` ...
- coordinator·executor 모두 상태를 **프로세스 메모리**에 두므로 인스턴스당 **단일 워커**로 실행한다. 처리량 확장은 워커가 아니라 **executor 인스턴스 수**로 한다.

## 빠른 설치 (스크립트 사용)

```bash
# 0) (최초 1회) Python 3.11 설치
sudo dnf install -y python3.11 python3.11-pip python3.11-devel rsync

# 1) 저장소 루트에서 실행
sudo ./deploy/install.sh

# 2) 서비스 기동 (executor 2개 + coordinator)
sudo systemctl enable --now query-executor@8001 query-executor@8002
sudo systemctl enable --now query-coordinator
```

`install.sh`가 하는 일:
- 서비스 계정 `queryexec` 생성
- 앱을 `/opt/query-executor` 로 복사(`.venv`/`.git` 제외)
- `/opt/query-executor/.venv` 가상환경 + `requirements.txt` 설치
- `/etc/query-executor/{coordinator,executor}.env` 배치(없을 때만)
- systemd 유닛 설치 후 `daemon-reload`

## 수동 설치

```bash
sudo useradd --system --home-dir /opt/query-executor --shell /sbin/nologin queryexec
sudo mkdir -p /opt/query-executor /etc/query-executor
sudo rsync -a --exclude '.venv' --exclude '.git' ./ /opt/query-executor/

sudo python3.11 -m venv /opt/query-executor/.venv
sudo /opt/query-executor/.venv/bin/pip install -r /opt/query-executor/requirements.txt
sudo chown -R queryexec:queryexec /opt/query-executor

sudo cp deploy/systemd/coordinator.env.example /etc/query-executor/coordinator.env
sudo cp deploy/systemd/executor.env.example /etc/query-executor/executor.env
sudo cp deploy/systemd/query-coordinator.service /etc/systemd/system/
sudo cp deploy/systemd/query-executor@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 운영 명령

```bash
# 상태/로그
systemctl status query-coordinator
journalctl -u query-coordinator -f
journalctl -u query-executor@8001 -f

# 재시작 / 중지
sudo systemctl restart query-coordinator
sudo systemctl stop query-executor@8002

# executor 인스턴스 추가(포트 8003)
#   coordinator.env 의 EXECUTORS 에 http://127.0.0.1:8003 추가 후
sudo systemctl enable --now query-executor@8003
sudo systemctl restart query-coordinator
```

## 동작 확인

```bash
curl -s localhost:8000/healthz
curl -s localhost:8001/healthz

curl -s localhost:8000/jobs -H 'content-type: application/json' -d '{
  "sql": "SELECT user_id, amount, dt FROM sales WHERE dt IN ('\''2026-01-01'\'','\''2026-01-02'\'') AND region='\''KR'\''",
  "partition_column": "dt",
  "target_table": "public.sales_mirror",
  "parallelism": 2
}'
```

## 방화벽(firewalld)

외부에서 coordinator(8000)에 접근해야 한다면:

```bash
sudo firewall-cmd --permanent --add-port=8000/tcp
sudo firewall-cmd --reload
```

executor 포트(8001, 8002 ...)는 보통 coordinator와 같은 호스트 내부 통신이므로 외부 개방이 불필요하다.

## 참고

- 기본 executor 백엔드는 `MockBackend`(실제 I/O 없음)다. 실제 Impala→Greenplum 적재는
  `executor/app.py` 에서 `ImpalaToGreenplumBackend` 를 환경변수로 주입하도록 연결하고
  `requirements-executor.txt` 를 설치해야 한다.
- coordinator를 다중 인스턴스로 띄우려면 Job 저장소를 Redis 등으로 교체해야 한다(현재 인메모리).
