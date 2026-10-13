# 부하 테스트 도구 기능 구성도 제작 기록

- 결과물: [07-load-test.png](07-load-test.png)
- 생성 방식: 내장 `image_gen` 도구.
- 근거: `src/tools/load/README.md`, `runner.py`, `analysis.py`.
- 스타일: 기존 [제안서 그림 기준](GENERATION.md)의 Data Dynamics Architecture Style 1.0 해석 초안. 공식 스타일 가이드 준수를 단정하지 않는다.
- 구성: 시나리오·요청 본문 치환 → 부하 곡선·VU closed loop → async/sync 완료 판정. 하단은 Baseline·Drain·Wrap Up, 측정 지표, 포화점 분석과 결과 파일.
- 포화 그래프는 개념도이며 성능 보장이나 실측 벤치마크가 아니다.
- 검토: 한글·자리표시자 가독성, 완료 후 다음 요청으로 돌아가는 VU 루프, async 제출과 완료 구분, 429 별도 집계, Drain 초과 시 ABANDONED 기록, 개념도 표기와 README 상대경로를 확인했다. PNG 크기는 1536×1024다.

## 생성 지시문

```text
Create one polished high-resolution landscape PNG technical architecture infographic for a Korean enterprise proposal, approximately 2400x1600 or larger. Match the existing project's interpretation of "Data Dynamics Architecture Style 1.0": pure white background, dark navy Korean typography, blue control arrows and blue outlined pale-blue cards, teal data/measurement cards and arrows, thin grey rounded group boundaries, orange exception paths, restrained flat 2D small icons, generous gutters and precise orthogonal connectors. NO overall title, NO subtitle/banner/header/brand/watermark. Local section labels allowed. Render all Korean and code text accurately, legibly, with large fonts. This is a detailed load testing tool, NOT a data loading architecture.
Layout: upper half three left-to-right connected columns; lower half three aligned explanatory panels; bottom small output strip. Keep every connector unambiguous.

Upper left group "시나리오 · 요청 준비": cards "wizard · CLI · JSON / YAML" with small "CLI 값 우선 적용"; card "요청 본문 치환" with EXACT literal examples "${vu} · ${iter} · ${seq} · ${uuid}" and "${choice:a,b,c} · ${randint:1-9}" and "${date:today-7} · ${data:region}". A small teal dataset card "CSV · JSONL · JSON · YAML" has arrow to body rendering labelled "요청마다 한 행". Short note "한 요청의 data 값은 같은 행 사용". Bottom example visibly paired as "dwtemp.load_${vu} → dwtemp.load_3". Connect rendered request rightward to VU pool.

Upper middle group "부하 제어 · VU 실행": small graph with axes "VU" and "시간", blue line ramping then flat, caption "Ramp-up · stages" and short "증가 · 유지 · 감소 / plan으로 사전 확인". Under graph three cards "VU 1", "VU 2", "VU N". Beneath these ONE shared explicit circular loop of THREE boxes "본문 생성" → "요청 전송" → "완료 대기" → back to "본문 생성"; label "각 VU는 완료 후 다음 요청". Important: next iteration only after completion. Note "VU당 한 요청 진행". Request arrows go from this group to API group on right.

Upper right group "Coordinator API · 완료 판정": two distinct lanes: blue "async /jobs": "POST → job_id → 상태 폴링 → 종료 확인", short note "DONE 성공 / PARTIAL · FAILED · CANCELLED 종료"; teal "sync /query-execute · /query-count": "요청 → 응답 수신 = 완료". Small orange chip "HTTP 429: 포화 신호" and note "오류율과 별도 집계". DO NOT treat accepted async submission as completed. A bottom small card "GET /cluster" with "Coordinator · Executor 자원 수집" returns dashed grey measurement arrow to lower measurement panel.

Lower left panel "실행 단계 · Wrap Up": clearly linear horizontal or stacked process "Baseline → 부하 실행 → Drain → 결과 저장". Under Baseline "기준선 10초", under Drain "새 요청 중단 · 진행 중 요청 대기". Orange branch from Drain labelled "제한 시간 초과" leading to box "async: cancel / abandon" with note "미완료는 ABANDONED 기록". Below short "Drain 기본 5분 · Ctrl-C 1회 정리 / 2회 즉시 종료". This panel explicitly describes teardown, no invented feature named wrap-up.

Lower middle panel "측정 · 라이브 모니터링": readable metric chips "제출 TPS · 완료 TPS", "e2e p50 / p95", "서버 대기 · 실행 시간", "적재 rows · rows/s", "CPU · 메모리 · 실행 Task". Short note "async 용량 판단은 완료 TPS 기준". Small cadence note "라이브 1초 · 자원 표본 기본 5초".

Lower right panel "포화점 분석 · saturation": miniature accurate conceptual chart y-axis "완료 TPS", x-axis "VU", teal line rising and flattening, orange dot at beginning of flat part labelled "무릎점"; caption "개념도 · 실측 수치 아님". Short bullets rendered as simple lines "VU 수준별 정상상태 TPS 비교", "초기 과도구간 기본 30% 제외", "인접 수준 처리량 증가율 5% 미만 판정". Do not fabricate numeric benchmark measurements.

Bottom full-width compact strip three output cards with teal connector from measurements and analysis: "콘솔 요약 · 라이브 패널"; "JSON: 설정 · 요약 · 시계열 · 자원"; "CSV · 포화 곡선 PNG / SVG". All features must be in a readable cohesive architecture drawing. No microscopic wall of text. No global heading.
```
