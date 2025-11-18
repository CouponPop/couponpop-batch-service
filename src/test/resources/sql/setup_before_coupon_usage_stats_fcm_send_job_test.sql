-- 배치 테스트용 스키마 초기화
DROP TABLE IF EXISTS coupon_events;
DROP TABLE IF EXISTS coupon_usage_stats;

CREATE TABLE coupon_usage_stats
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id     BIGINT       NOT NULL,
    top_dong      VARCHAR(255) NOT NULL,
    top_hour      INT          NOT NULL,
    aggregated_at DATE         NOT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE coupon_events
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    store_id       BIGINT   NOT NULL,
    event_start_at DATETIME NOT NULL,
    event_end_at   DATETIME NOT NULL,
    total_count    INT      NOT NULL,
    issued_count   INT      NOT NULL
);
