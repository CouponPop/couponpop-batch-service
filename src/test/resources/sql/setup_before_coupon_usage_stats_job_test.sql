-- coupon_usage 테이블 생성
DROP TABLE IF EXISTS coupon_usage;
CREATE TABLE coupon_usage
(
    id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id BIGINT       NOT NULL,
    coupon_id BIGINT       NOT NULL,
    store_id  BIGINT       NOT NULL,
    dong      VARCHAR(255) NOT NULL COMMENT '쿠폰 사용 매장 동 정보',
    used_at   DATETIME     NOT NULL COMMENT '쿠폰 사용 일시',

    INDEX idx_member_id (member_id),
    INDEX idx_coupon_id (coupon_id),
    INDEX idx_store_id (store_id),
    INDEX idx_used_at_member (used_at, member_id)
);

-- coupon_usage_stats 테이블 생성
DROP TABLE IF EXISTS coupon_usage_stats;
CREATE TABLE coupon_usage_stats
(
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id  BIGINT       NOT NULL COMMENT '손님 ID',
    top_dong   VARCHAR(255) NOT NULL COMMENT '쿠폰 사용 상위 동 정보',
    top_hour   INT          NOT NULL COMMENT '쿠폰 사용 상위 시간대(0~23)',
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '등록일',
    updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일',

    UNIQUE KEY uk_member_id (member_id),

    INDEX idx_top_dong (top_dong),
    INDEX idx_top_hour (top_hour)
);
