# 제안서 그림 제작 기록

이 파일은 그림 유지보수를 위한 제작 기록이다. 제안서 본문은 [TECHNICAL_PROPOSAL.md](../../TECHNICAL_PROPOSAL.md)에 있다.

## 공통 기준

- 생성 방식은 내장 `image_gen` 도구이며 CLI/API 대체 경로는 사용하지 않았다.
- 결과물은 PNG 6장이다. 그림 상단에는 전체 제목·부제·브랜드 문구를 넣지 않는다.
- 구성요소 이름과 영역 이름은 그림 내부에 표시하며, 그림 번호와 제목은 제안서 본문에 둔다.
- Data Dynamics Architecture Style 1.0의 공식 가이드는 확인되지 않았다. 흰 배경·남색 글자·파란 제어선·청록 데이터선의 스타일 해석 초안이다.
- 수정은 이미지 생성 도구로 수행했으며 생성 결과를 자르거나 글자를 덧씌우지 않았다.

## 이미지 목록

| 파일 | 설명 |
|---|---|
| [01-overview.png](01-overview.png) | 전체 구성 |
| [02-partition-split.png](02-partition-split.png) | 파티션 분할 |
| [03-load-modes.png](03-load-modes.png) | 적재 방식 비교 |
| [04-staging-phases.png](04-staging-phases.png) | 스테이징 단계 |
| [05-high-availability.png](05-high-availability.png) | 고가용성 |
| [06-concurrency.png](06-concurrency.png) | 동시성 제어 |

## 검토 사항

상단 전체 제목의 부재, 한글 표기, 실제 데이터의 Coordinator 우회, 다섯 적재 경로의 차이,
export와 최종 적재의 구분, 장애 후 상태 정합과 retry, 대기 큐에서 실행 슬롯으로의 진행 방향을
그림별로 확인했다. 파티션 그림은 문장과 상태선을 수정하고, 동시성 그림은 대기 방향과 429 분기점을 수정했다.

## 생성 지시문

### 01. 전체 구성

```text
Use case: infographic-diagram. Create a polished Korean enterprise technical proposal architecture diagram as a landscape PNG. White background, dark navy labels, blue control services, teal data services, light grey rounded boundaries, ample whitespace, precise orthogonal connectors, restrained flat 2D design. This is a provisional Data Dynamics architecture style 1.0 interpretation.
CRITICAL: NO title, NO heading, NO subtitle, NO brand, NO header strip at the top of the image, NO footer title, NO watermark. The image starts immediately with diagram objects. Only component names, short explanatory labels and a small bottom legend.
Diagram content: top row left box "업무 시스템" subtitle "작업 요청 · 상태 조회", center larger box "Coordinator" subtitle "검증 · 분할 · 작업 배정", right cylinder "공유 PostgreSQL" subtitle "Job · Task 이력". Thin blue bidirectional control connection between client and Coordinator labelled "REST API"; thin blue Coordinator to PostgreSQL labelled "상태 저장".
Second row: left database "Impala" subtitle "원본 데이터", center bounded pool labelled "Executor" containing three equal small server cards "실행 노드 1", "실행 노드 2", "실행 노드 N", right database "Greenplum / WarehousePG" subtitle "대상 데이터". Thick teal arrows strictly left-to-right from Impala into Executor pool and Executor pool into target database, labelled "병렬 읽기" and "COPY 적재".
Thin blue control arrow from Coordinator down to executor pool labelled "작업 지시"; a separate grey dotted arrow from executor pool upward to Coordinator labelled "상태 · 행 수". NO data arrows through Coordinator. Diagram illustrates copy mode. Bottom small note "copy 모드 기준 · 대량 데이터는 Executor를 통해 이동합니다." Small legend blue thin line "제어", teal thick line "데이터", grey dashed line "상태". Render exact Korean text legibly. High resolution.
```

### 02. 파티션 분할

```text
Use case: infographic-diagram. Produce ONE high-resolution landscape PNG for a Korean enterprise technical proposal. White background, navy text, blue control components, teal data components, thin light-grey rounded boundaries, small flat server/database icons, ample whitespace and precise orthogonal connectors. Visually match a restrained Data Dynamics architecture style 1.0 draft. Korean labels must be spelled exactly and clearly. CRITICAL: NO overarching title, NO page heading, NO subtitle or header at the top, NO brand, NO watermark. Start directly with the diagram objects. Component names and local lane labels are permitted. Avoid excessive small text. Show a left-to-right three-column partition splitting diagram. Left a blue box "이관 요청" with "파티션 6개 · parallelism = 3" and six separate date tokens "10/01", "10/02", "10/03", "10/04", "10/05", "10/06". Below the tokens say "예시: 날짜별 매출 데이터". Center a branching blue connector to three vertically stacked task cards: "Task 1" with "10/01 · 10/02", "Task 2" with "10/03 · 10/04", "Task 3" with "10/05 · 10/06". Each card has its own teal arrow rightwards to a matching disjoint partition row inside one target database container "대상 테이블". The three target rows have exactly the same date pairs respectively. Bottom separate small grey box "Coordinator" with "상태 · 적재 행 수 집계" and grey dashed incoming status arrows from the three task cards. Large clear note at bottom "파티션은 겹치지 않게 나눕니다. 실제 데이터는 각 Task가 독립 적재합니다." Do not show any data flowing through Coordinator. Do not imply global result merge or one executor per task guaranteed.
```

수정 지시문:

```text
Preserve the composition, colors, Korean component labels, all dates and teal data arrows. Correct only the bottom explanatory sentence to EXACTLY '파티션은 겹치지 않게 나눕니다. 각 Task는 담당 범위를 독립적으로 적재합니다.' Make it fully readable without missing words. Also reroute the three grey dashed status connectors so each goes from its own task card separately to the Coordinator without passing through any other task box: route from the right edge of each task into the gap left of the target table, then down to Coordinator. These grey status arrows must NOT connect Task 1 to Task 2 or Task 2 to Task 3. No global title or top heading. Output PNG.
```

수정 지시문:

```text
Edit this diagram surgically. Preserve everything including the exact Korean bottom sentence. There are now FOUR grey dashed arrowheads entering the Coordinator box. Remove ONLY the extra short central vertical dashed connector running from the bottom-center of Task 3 straight down to the top-center of Coordinator (at about 50% of image width). Keep the leftmost short dashed connector from Task 3, the bent connector from Task 2 on the right, and the far-right long connector from Task 1. The final diagram must have EXACTLY THREE grey status arrows entering Coordinator, one from each task. No other changes, no title or heading added. PNG output.
```

### 03. 적재 방식 비교

```text
Use case: infographic-diagram. Produce ONE high-resolution landscape PNG for a Korean enterprise technical proposal. White background, navy text, blue control components, teal data components, thin light-grey rounded boundaries, small flat server/database icons, ample whitespace and precise orthogonal connectors. Visually match a restrained Data Dynamics architecture style 1.0 draft. Korean labels must be spelled exactly and clearly. CRITICAL: NO overarching title, NO page heading, NO subtitle or header at the top, NO brand, NO watermark. Start directly with the diagram objects. Component names and local lane labels are permitted. Avoid excessive small text. Five evenly spaced horizontal swimlanes, each with a short left mode label and clear teal data arrows. No global heading. Lane 1 label "copy", flow "Impala" → "Executor" → "대상 테이블", arrow from executor labelled "COPY". Lane 2 label "statement", flow inside one large grey boundary "Greenplum / WarehousePG": "소스 테이블" → "INSERT SELECT" → "대상 테이블"; small blue Executor box outside above this lane sends thin blue arrow "SQL 지시" to INSERT SELECT. No Impala data flow for this lane. Lane 3 label "stage_insert", flow "Impala" → "Executor" → "DB staging" → "대상 테이블", last two arrows labelled "COPY" and "INSERT". Lane 4 label "local_stage", flow "Impala" → "Executor" → "로컬 CSV" → "GP 세그먼트", boundary enclosing Executor, CSV and segment labelled "같은 세그먼트 호스트", last arrow labelled "file://". Lane 5 label "s3_stage", flow "Impala" → "Executor" → "S3" → "GP 세그먼트", last arrow labelled "PXF". At bottom a small blue note "local_stage · s3_stage: 모든 export 완료 후 Coordinator가 적재 SQL을 실행합니다." Small legend teal "데이터" blue "제어". Keep arrows and labels distinct, no text crossing arrows.
```

### 04. 스테이징 단계

```text
Use case: infographic-diagram. Produce ONE high-resolution landscape PNG for a Korean enterprise technical proposal. White background, navy text, blue control components, teal data components, thin light-grey rounded boundaries, small flat server/database icons, ample whitespace and precise orthogonal connectors. Visually match a restrained Data Dynamics architecture style 1.0 draft. Korean labels must be spelled exactly and clearly. CRITICAL: NO overarching title, NO page heading, NO subtitle or header at the top, NO brand, NO watermark. Start directly with the diagram objects. Component names and local lane labels are permitted. Avoid excessive small text. Create a left-to-right three-stage workflow with no global title. First large lane labelled locally "① 병렬 export" contains three stacked Executor cards each with a teal arrow to small "CSV / S3" file/storage icon. Next a slim central vertical blue gate labelled "모든 export 성공" with blue control arrow to next large box "② DB 적재". In the DB load box show top small control card "Coordinator" connected by thin blue arrow labelled "SQL 실행" to lower database "Greenplum / WarehousePG"; show teal incoming arrow from CSV / S3 at bottom into the database labelled "세그먼트 병렬 읽기". Coordinator must not be on the teal data route. Inside DB lower small text "외부테이블 → INSERT → COMMIT". Third far-right box "③ 임시 데이터 정리" linked after database by blue arrow labelled "적재 완료 후", containing "로컬 파일 / S3 객체" and small clean-up icon. Below first gate orange failure branch to orange box "export 실패 또는 취소" then "적재 단계 진입 중단". Small bottom note "①의 완료는 이관 완료가 아닙니다. ②의 적재 결과까지 확인합니다." NO prominent heading above stages. Avoid circular data routes and ambiguity about transfer direction.
```

### 05. 고가용성

```text
Use case: infographic-diagram. Produce ONE high-resolution landscape PNG for a Korean enterprise technical proposal. White background, navy text, blue control components, teal data components, thin light-grey rounded boundaries, small flat server/database icons, ample whitespace and precise orthogonal connectors. Visually match a restrained Data Dynamics architecture style 1.0 draft. Korean labels must be spelled exactly and clearly. CRITICAL: NO overarching title, NO page heading, NO subtitle or header at the top, NO brand, NO watermark. Start directly with the diagram objects. Component names and local lane labels are permitted. Avoid excessive small text. Draw a high availability control architecture, no global heading. Left box "업무 시스템" connects to small dashed-outline box "접속 분산 계층" subtitle "운영 인프라 구성" then branches to two same-size blue server boxes stacked vertically "Coordinator A" and "Coordinator B". Center-right shared database cylinder "공유 PostgreSQL" subtitle "Job 상태 · heartbeat · 실행 부하"; each Coordinator has a blue bidirectional connector to this database. Far right a bounded pool "Executor" containing three server icons, with grey dashed arrow from pool to shared database labelled "상태 직접 보고". Thin blue command connections from both coordinators to pool routed above and below database, labelled "작업 배정". Do not include source/target data databases because this is control topology only. Along bottom a distinct four-step orange recovery sequence with arrows: "소유 Coordinator 장애" → "heartbeat 만료 감지" → "미완료 Job을 FAILED로 정합" → "적재 상태 확인 후 retry". Small bottom note "실행 중 작업의 자동 승계가 아니라, 상태 정합 후 재처리하는 구조입니다." Distinguish shared PostgreSQL with blue/grey not teal. Keep Korean readable and no duplicate steps.
```

### 06. 동시성 제어

```text
Use case: infographic-diagram. Produce ONE high-resolution landscape PNG for a Korean enterprise technical proposal. White background, navy text, blue control components, teal data components, thin light-grey rounded boundaries, small flat server/database icons, ample whitespace and precise orthogonal connectors. Visually match a restrained Data Dynamics architecture style 1.0 draft. Korean labels must be spelled exactly and clearly. CRITICAL: NO overarching title, NO page heading, NO subtitle or header at the top, NO brand, NO watermark. Start directly with the diagram objects. Component names and local lane labels are permitted. Avoid excessive small text. Draw a left-to-right capacity-control pipeline with four clear columns and no global heading. Far left small input "업무 요청" entering first large blue bounded column "Job 수용 제어" containing "실행 슬롯" above "대기 큐"; small orange rejected side branch downwards to "용량 초과: HTTP 429". Arrow into second blue column "Task 배정 제어" subtitle "동시 dispatch 제한". Arrow into third teal column "Executor 실행 제어" containing three server cards and subtitle "노드별 동시 Task 제한". Arrow into fourth grey/teal column "원본 · 대상 DB" containing two database cylinders "Impala" and "Greenplum" with subtitle "쿼리 슬롯 · 연결 수 · I/O". Main arrows should mean work progression, blue, with label "작업 진행" in small legend. Along bottom wide subtle panel contains exactly these two short notes, not a title: "수용 한도는 Coordinator 인스턴스별로 적용됩니다." and "확장 한계는 원본 DB·대상 DB·네트워크의 여유 용량에 따라 달라집니다." Bottom small monitoring strip with four separate metric chips "완료 TPS", "대기 시간", "429 비율", "CPU · 메모리". Do not invent benchmark numbers or capacity guarantees.
```

수정 지시문:

```text
Preserve the entire composition and all Korean labels, colors and monitoring strip. Fix only the queue/admission arrows in first column. REMOVE the downward blue arrow from 실행 슬롯 to 대기 큐. Instead draw a clear upward blue arrow from 대기 큐 toward 실행 슬롯 with a small label '슬롯 확보 후'. The orange rejection connector must originate from the left boundary of the 'Job 수용 제어' large column, route downward in the gap, then point to the orange '용량 초과: HTTP 429' card. REMOVE the orange arrow directly from 업무 요청 to rejection card. It is admission control that rejects the request, not the client. No added top title or overarching header. Output PNG.
```
