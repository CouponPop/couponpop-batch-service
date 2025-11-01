package com.couponpop.batchservice.domain.coupon.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Objects;

@Repository
@RequiredArgsConstructor
public class CouponUsageStatsFcmSendJdbcRepository {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    /**
     * 동 기준으로 현재 진행 중인 쿠폰 이벤트 수를 카운트한다.
     *
     * @param dong          조회 대상 동
     * @param referenceTime 진행 중 여부를 판단할 기준 시각
     * @return 진행 중인 이벤트 개수
     */
    public int countActiveCouponEventsByDong(String dong, LocalDateTime referenceTime) {
        String sql = """
                SELECT COUNT(ce.id)
                FROM coupon_events ce
                   INNER JOIN stores s ON s.id = ce.store_id
                WHERE s.dong = :dong
                   AND :now BETWEEN ce.event_start_at AND ce.event_end_at
                   AND total_count > issued_count
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("dong", dong)
                .addValue("now", Timestamp.valueOf(referenceTime));

        Integer result = namedParameterJdbcTemplate.queryForObject(sql, params, Integer.class);
        return Objects.requireNonNullElse(result, 0);
    }
}
