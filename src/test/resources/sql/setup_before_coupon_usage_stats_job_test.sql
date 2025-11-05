-- coupon_histories 테이블 생성
DROP TABLE IF EXISTS coupon_histories;
CREATE TABLE coupon_histories
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id       BIGINT                              NOT NULL,
    coupon_id       BIGINT                              NOT NULL,
    store_id        BIGINT                              NOT NULL,
    coupon_event_id BIGINT                              NOT NULL,
    coupon_status   ENUM ('ISSUED', 'USED', 'CANCELED') NOT NULL,
    created_at      DATETIME                            NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- coupon_usage_stats 테이블 생성
DROP TABLE IF EXISTS coupon_usage_stats;
CREATE TABLE coupon_usage_stats
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id     BIGINT       NOT NULL COMMENT '손님 ID',
    top_dong      VARCHAR(255) NOT NULL COMMENT '쿠폰 사용 상위 동 정보',
    top_hour      INT          NOT NULL COMMENT '쿠폰 사용 상위 시간대(0~23)',
    aggregated_at DATE         NOT NULL COMMENT '집계 날짜',
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '등록일'
);
