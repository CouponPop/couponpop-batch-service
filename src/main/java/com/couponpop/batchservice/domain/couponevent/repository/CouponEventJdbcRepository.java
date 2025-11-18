package com.couponpop.batchservice.domain.couponevent.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

@Repository
@RequiredArgsConstructor
public class CouponEventJdbcRepository {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public int countActiveCouponEventsByStoreIds(List<Long> storeIds, LocalDateTime referenceTime) {
        String sql = """
                SELECT COUNT(ce.id)
                FROM coupon_events ce
                WHERE ce.store_id in (:storeIds)
                   AND :now BETWEEN ce.event_start_at AND ce.event_end_at
                   AND total_count > issued_count
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("storeIds", storeIds)
                .addValue("now", Timestamp.valueOf(referenceTime));

        Integer result = namedParameterJdbcTemplate.queryForObject(sql, params, Integer.class);
        return Objects.requireNonNullElse(result, 0);
    }
}
