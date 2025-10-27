package com.sparta.couponpopbatch.domain.member.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;

@Repository
@RequiredArgsConstructor
public class MemberFcmTokenJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * 오래 사용하지 않은 FCM 토큰 삭제
     *
     * @return 삭제된 행의 수
     */
    public int deleteUnusedFcmTokensBefore(LocalDateTime threshold) {
        String sql = """
                    DELETE FROM member_fcm_tokens
                    WHERE last_used_at IS NULL
                       OR last_used_at < ?
                """;

        Timestamp timestamp = Timestamp.valueOf(threshold);

        return jdbcTemplate.update(sql, timestamp);
    }

}
