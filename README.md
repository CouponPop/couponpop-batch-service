# couponpop-batch-service

Spring Boot 3 기반 배치 서비스로, 회원의 쿠폰 사용 이력을 집계하고 개인화된 푸시 발송을 위한 데이터를 만들어 RabbitMQ로 전달합니다. 두 개의 Spring Batch Job을 주기적으로(일별/시간별) 실행하며, 시스템 토큰을 사용하는 Feign Client로 다른 CouponPop 마이크로서비스와 통신합니다.

## 주요 역할

- `coupon_histories` 테이블을 읽어 회원별 최다 이용 동(행정동)과 사용 시간이 언제인지 분석합니다.
- 집계 결과를 `coupon_usage_stats` 테이블에 저장하여 후속 서비스가 사용할 수 있도록 합니다.
- 매장 서비스/알림 서비스와 통신해 동 정보와 활성 쿠폰 이벤트 수를 조회하고, RabbitMQ를 통해 FCM 발송 요청 메시지를 게시합니다.
- 관리자 전용 HTTP API와 Prometheus 지표를 노출해 운영자가 수동 재실행이나 모니터링을 할 수 있도록 지원합니다.

## 배치 잡 개요

### `couponUsageStatsJob`

| 항목        | 내용                                                                            |
|-----------|-------------------------------------------------------------------------------|
| 목적        | 직전 20일간 `coupon_histories`를 스캔하여 5회 이상 사용한 회원의 최상위 동/시간 조합을 계산                |
| Reader    | `CouponHistoryJdbcRepository` + `StoreSystemFeignClient` (매장 동 정보를 청크 단위로 조회) |
| Processor | 메모리에서 회원별 사용 내역을 그룹화하고 가장 많이 사용한 동과 시간대를 선정                                   |
| Writer    | `coupon_usage_stats (member_id, top_dong, top_hour, aggregated_at)` INSERT    |
| 파라미터      | `runDate` (LocalDate). 스케줄러는 매일 01시에 실행하며 전날 날짜를 전달                           |

### `couponUsageStatsFcmSendJob`

| 항목        | 내용                                                                                                                                                    |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| 목적        | 특정 시간대(`targetHour`)에 대한 최신 집계 결과를 읽고 해당 회원들에게 FCM 발송 요청을 팬아웃                                                                                         |
| Reader    | `coupon_usage_stats`에서 `runDate-2일`~`runDate` 구간 + `top_hour == targetHour` 조건으로 조회                                                                   |
| Writer 흐름 | 알림 서비스에서 FCM 토큰 조회 → 매장 서비스에서 동별 매장 ID 조회 → `coupon_events`에서 활성 이벤트 개수 계산 → `coupon.usage.stats.fcm.send` 라우팅 키로 `CouponUsageStatsFcmSendMessage` 게시 |
| 내결함성      | 청크 사이즈 1,000, Feign/DataAccess 예외 최대 3회 재시도(지수 백오프), SkipListener 경고 로그, `NotificationTraceIdGenerator`로 추적 ID 부여                                     |
| 파라미터      | `runDate`(기본값=현재일), `targetHour`(기본값=현재 시각). 스케줄러는 매 정시마다 실행                                                                                          |

## 스케줄러 & 수동 실행

- 스케줄러는 `com.couponpop.batchservice.scheduler` 패키지에 있으며 `local`/`prod` 프로파일에서 활성화됩니다.
    - `CouponUsageStatsScheduler`: `0 0 1 * * *`
    - `CouponUsageStatsFcmSendScheduler`: `0 0 * * * *`
- 관리자 권한 JWT(`memberType=ADMIN/ROLE_ADMIN`)가 필요하며, 다음처럼 호출할 수 있습니다.

```bash
# 특정 날짜 집계 배치 강제 실행
curl -X POST 'http://localhost:8085/api/v1/jobs/coupon-usage-stats?runDate=2024-12-01' \
  -H 'Authorization: Bearer <admin-token>'

# 특정 날짜/시간에 대한 FCM 팬아웃 재실행
curl -X POST 'http://localhost:8085/api/v1/jobs/coupon-usage-stats-fcm-send?runDate=2024-12-01&targetHour=18' \
  -H 'Authorization: Bearer <admin-token>'
```

## 아키텍처 & 의존성

- Spring Boot 3.5, Spring Batch, Spring Data JDBC/JPA, OpenFeign, RabbitMQ, Micrometer Prometheus, (운영) AWS Parameter Store.
- 공유 모듈: `couponpop-core`(DTO·Rabbit 상수·Trace ID), `couponpop-security`(JWT 파싱, `@CurrentMember`, 시스템 토큰 인터셉터).
- 인프라: MySQL(업무 데이터 + 배치 메타), RabbitMQ(`coupon` 익스체인지), 필요 시 AWS 리소스.
- 모니터링: `/actuator/health`, `/actuator/prometheus`(공통 태그 `application=batch-service`, `MonitoringConfig` 참고).

## 환경 변수

로컬에서는 `couponpop-batch-service/.env`에 정의하고, 운영에선 Parameter Store를 사용합니다.

| 변수                                                                         | 설명                                     |
|----------------------------------------------------------------------------|----------------------------------------|
| `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`                                     | MySQL 접속 정보                            |
| `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` | RabbitMQ 접속 정보                         |
| `JWT_SECRET_KEY`                                                           | `couponpop-security`에서 사용하는 대칭키        |
| `client.store-service.url`, `client.notification-service.url`              | 필요 시 다른 시스템의 엔드포인트로 오버라이드              |
| `GITHUB_ACTOR`, `GITHUB_TOKEN`                                             | Gradle이 사설 core/security 모듈을 내려받을 때 필요 |

### 샘플 `.env`

```dotenv
DB_URL=jdbc:mysql://localhost:3307/coupon_db
DB_USERNAME=root
DB_PASSWORD=1234
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=admin
RABBITMQ_PASSWORD=admin
JWT_SECRET_KEY=local-dev-secret
GITHUB_ACTOR=<your-github-id>
GITHUB_TOKEN=<github-personal-access-token>
```

## 로컬 개발 절차

1. **사전 준비**: JDK 17, Docker, MySQL/RabbitMQ 인프라. 루트의 `local-docker-infra/docker-compose.yml`로 Redis·RabbitMQ를 띄우고, MySQL은 별도 compose(예: `local-docker-infra/docker-compose.db.replica.yml`)를 활용합니다.
2. **의존성 다운로드**: `./gradlew :couponpop-batch-service:dependencies` (최초 실행 시 GitHub 토큰 필요).
3. **애플리케이션 실행**
   ```bash
   cd couponpop-batch-service
   ./gradlew bootRun --args='--spring.profiles.active=local'
   ```
   로컬 포트는 `8085`, 운영 포트는 `8080`입니다.
4. **테스트 실행**: `./gradlew test` (Spring Batch 테스트 + JaCoCo 리포트는 `build/reports/jacoco/test/html`).
5. **잡 상태 확인**: Actuator `/actuator/batch/jobs/**` 또는 Spring Batch 메타 테이블(`BATCH_JOB_INSTANCE` 등)을 조회합니다.

## 운영 시 참고

- **재시도·멱등성**: Feign/Rabbit 호출은 최대 3회 재시도 후 Skip 처리하며, 실패한 회원 ID를 경고 로그로 남깁니다. 동일한 `runDate`/`targetHour`로 재실행하면 빠진 항목을 다시 처리할 수 있습니다.
- **Trace ID**: `CouponUsageStatsFcmSendMessage`는 `NotificationTraceIdGenerator`로 생성된 deterministic trace ID를 묶어 알림 서비스에서 Redis 멱등성을 보장합니다.
- **모니터링**: `/actuator/prometheus`에서 배치 실행 카운터, 소요 시간 등을 수집할 수 있으며, JWT 화이트리스트에 등록되어 있어 무인 노출이 가능합니다.
- **장애 대응**: 외부 시스템 오류로 잡이 멈추지 않도록 skip 정책을 적용했으므로, 장애 복구 후 필요 시 관리자 API로 재실행하세요.
