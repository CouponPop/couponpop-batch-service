package com.couponpop.batchservice.domain.couponhistory.repository;

import com.couponpop.batchservice.domain.coupon.enums.CouponStatus;
import com.couponpop.batchservice.domain.couponhistory.repository.projection.CouponHistoryUsedInfoProjection;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class CouponHistoryJdbcRepository {

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    public List<CouponHistoryUsedInfoProjection> findCouponHistoriesUsedInfo(CouponStatus status, LocalDateTime from, LocalDateTime to) {

        String sql = """
                SELECT ch.id AS couponHistoryId,
                       ch.store_id AS storeId,
                       ch.member_id AS memberId,
                       ch.created_at AS createdAt
                FROM coupon_histories ch
                WHERE ch.coupon_status = :status
                  AND ch.created_at BETWEEN :from AND :to
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("status", status.name())
                .addValue("from", from)
                .addValue("to", to);

        return namedParameterJdbcTemplate.query(sql, params, new DataClassRowMapper<>(CouponHistoryUsedInfoProjection.class));
    }

}
