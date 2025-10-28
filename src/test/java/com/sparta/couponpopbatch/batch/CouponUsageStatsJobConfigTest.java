package com.sparta.couponpopbatch.batch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBatchTest
@SpringBootTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(scripts = "/sql/setup_before_coupon_usage_stats_job_test.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
@Sql(scripts = "/sql/cleanup_after_coupon_usage_stats_job_test.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
class CouponUsageStatsJobConfigTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("couponUsageStatsJob")
    private Job couponUsageStatsJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(couponUsageStatsJob);
    }

    @Test
    @DisplayName("쿠폰 사용 기록을 집계하면 손님별 가장 많이 사용한 동과 시간대가 저장된다.")
    void runCouponUsageStatsJob_success_recordsWithinRange() throws Exception {
        // given
        insertCouponUsageFixtures(); // 테스트용 쿠폰 사용 기록 삽입

        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.of(2025, 10, 31)) // 2025년 10월 데이터까지 집계
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        List<CouponUsageStatsRow> statsRows = jdbcTemplate.query(
                "SELECT member_id, top_dong, top_hour FROM coupon_usage_stats ORDER BY member_id",
                (rs, rowNum) -> new CouponUsageStatsRow(
                        rs.getLong("member_id"),
                        rs.getString("top_dong"),
                        rs.getInt("top_hour")
                )
        );

        assertThat(statsRows).hasSize(3);

        /*
         * - 동 카운트(요약): 상도동 2, 잠실동 2, 대치동 2, 서교동 2, 그 외 각 1
         * - top_dong = "서교동" (동률 2이지만 가장 최근 `2025-10-26T11:00:00`로 타이브레이크)
         * - top_hour = 11 (서교동 11시에 2건 -> 최다)
         */
        assertThat(statsRows)
                .filteredOn(row -> row.memberId() == 1L)
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.topDong()).isEqualTo("서교동");
                    assertThat(row.topHour()).isEqualTo(11);
                });

        /*
         * - 동 카운트(요약): 상도동 2(최신 10-25), 대치동 2(최신 10-20), 잠실동 2(최신 10-23), 흑석동 2(최신 10-18), 그 외 각 1
         * - top_dong = "상도동" (동률 2 중 가장 최신 `2025-10-25T13:00:00`)
         * - top_hour = "13" (상도동 13시 1건 vs 12시 1건 → 최신 max_used_at으로 13시 선택)
         */
        assertThat(statsRows)
                .filteredOn(row -> row.memberId() == 2L)
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.topDong()).isEqualTo("상도동");
                    assertThat(row.topHour()).isEqualTo(13);
                });

        /*
         * - 동 카운트(요약): 노량진동 19(전부 15:00), 상도동 1, 흑석동 1, 잠실동 1
         * - top_dong = "노량진동" (압도적 다수)
         * - top_hour = 15 (노량진동 전부 15시 사용)
         */
        assertThat(statsRows)
                .filteredOn(row -> row.memberId() == 3L)
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.topDong()).isEqualTo("노량진동");
                    assertThat(row.topHour()).isEqualTo(15);
                });
    }

    private void insertCouponUsageFixtures() {
        insertCouponUsage(1, 200, 31, "노량진동", parseDateTime("2025-10-07 08:15:00"));
        insertCouponUsage(1, 201, 32, "상도동", parseDateTime("2025-10-08 12:30:00"));
        insertCouponUsage(1, 202, 33, "흑석동", parseDateTime("2025-10-09 16:10:00"));
        insertCouponUsage(1, 203, 34, "잠실동", parseDateTime("2025-10-10 18:30:00"));
        insertCouponUsage(1, 204, 35, "대치동", parseDateTime("2025-10-11 09:40:00"));
        insertCouponUsage(1, 205, 36, "역삼동", parseDateTime("2025-10-12 21:05:00"));
        insertCouponUsage(1, 206, 37, "서교동", parseDateTime("2025-10-13 11:00:00"));
        insertCouponUsage(1, 207, 38, "연남동", parseDateTime("2025-10-14 15:00:00"));
        insertCouponUsage(1, 208, 39, "상도동", parseDateTime("2025-10-20 09:00:00"));
        insertCouponUsage(1, 209, 34, "잠실동", parseDateTime("2025-10-22 13:00:00"));
        insertCouponUsage(1, 210, 35, "대치동", parseDateTime("2025-10-24 18:30:00"));
        insertCouponUsage(1, 211, 37, "서교동", parseDateTime("2025-10-26 11:00:00"));
        insertCouponUsage(1, 180, 31, "노량진동", parseDateTime("2025-09-20 10:00:00"));
        insertCouponUsage(1, 181, 32, "상도동", parseDateTime("2025-09-22 14:00:00"));

        insertCouponUsage(2, 300, 43, "흑석동", parseDateTime("2025-10-06 09:00:00"));
        insertCouponUsage(2, 301, 34, "잠실동", parseDateTime("2025-10-07 13:00:00"));
        insertCouponUsage(2, 302, 35, "대치동", parseDateTime("2025-10-08 18:30:00"));
        insertCouponUsage(2, 303, 36, "역삼동", parseDateTime("2025-10-09 21:05:00"));
        insertCouponUsage(2, 304, 37, "서교동", parseDateTime("2025-10-10 11:00:00"));
        insertCouponUsage(2, 305, 38, "연남동", parseDateTime("2025-10-11 15:00:00"));
        insertCouponUsage(2, 306, 32, "상도동", parseDateTime("2025-10-15 12:30:00"));
        insertCouponUsage(2, 307, 33, "흑석동", parseDateTime("2025-10-18 16:10:00"));
        insertCouponUsage(2, 308, 35, "대치동", parseDateTime("2025-10-20 18:30:00"));
        insertCouponUsage(2, 309, 34, "잠실동", parseDateTime("2025-10-23 09:40:00"));
        insertCouponUsage(2, 310, 32, "상도동", parseDateTime("2025-10-25 13:00:00"));
        insertCouponUsage(2, 311, 31, "노량진동", parseDateTime("2025-10-26 18:30:00"));
        insertCouponUsage(2, 182, 33, "흑석동", parseDateTime("2025-09-23 16:30:00"));
        insertCouponUsage(2, 183, 34, "잠실동", parseDateTime("2025-09-24 18:30:00"));

        insertCouponUsage(3, 400, 31, "노량진동", parseDateTime("2025-10-06 15:00:00"));
        insertCouponUsage(3, 401, 31, "노량진동", parseDateTime("2025-10-07 15:00:00"));
        insertCouponUsage(3, 402, 31, "노량진동", parseDateTime("2025-10-08 15:00:00"));
        insertCouponUsage(3, 403, 31, "노량진동", parseDateTime("2025-10-09 15:00:00"));
        insertCouponUsage(3, 404, 31, "노량진동", parseDateTime("2025-10-10 15:00:00"));
        insertCouponUsage(3, 405, 31, "노량진동", parseDateTime("2025-10-11 15:00:00"));
        insertCouponUsage(3, 406, 31, "노량진동", parseDateTime("2025-10-12 15:00:00"));
        insertCouponUsage(3, 407, 31, "노량진동", parseDateTime("2025-10-13 15:00:00"));
        insertCouponUsage(3, 408, 31, "노량진동", parseDateTime("2025-10-14 15:00:00"));
        insertCouponUsage(3, 409, 31, "노량진동", parseDateTime("2025-10-15 15:00:00"));
        insertCouponUsage(3, 410, 31, "노량진동", parseDateTime("2025-10-16 15:00:00"));
        insertCouponUsage(3, 411, 31, "노량진동", parseDateTime("2025-10-17 15:00:00"));
        insertCouponUsage(3, 412, 31, "노량진동", parseDateTime("2025-10-18 15:00:00"));
        insertCouponUsage(3, 413, 31, "노량진동", parseDateTime("2025-10-19 15:00:00"));
        insertCouponUsage(3, 414, 31, "노량진동", parseDateTime("2025-10-20 15:00:00"));
        insertCouponUsage(3, 415, 31, "노량진동", parseDateTime("2025-10-21 15:00:00"));
        insertCouponUsage(3, 416, 31, "노량진동", parseDateTime("2025-10-22 15:00:00"));
        insertCouponUsage(3, 417, 31, "노량진동", parseDateTime("2025-10-23 15:00:00"));
        insertCouponUsage(3, 418, 31, "노량진동", parseDateTime("2025-10-26 15:00:00"));
        insertCouponUsage(3, 419, 32, "상도동", parseDateTime("2025-10-24 09:40:00"));
        insertCouponUsage(3, 420, 33, "흑석동", parseDateTime("2025-10-25 18:30:00"));
        insertCouponUsage(3, 421, 34, "잠실동", parseDateTime("2025-10-26 21:05:00"));
        insertCouponUsage(3, 184, 35, "대치동", parseDateTime("2025-09-25 12:00:00"));
        insertCouponUsage(3, 185, 36, "역삼동", parseDateTime("2025-09-26 09:20:00"));
    }

    private void insertCouponUsage(long memberId, long couponId, long storeId, String dong, LocalDateTime usedAt) {
        jdbcTemplate.update(
                "INSERT INTO coupon_usage (member_id, coupon_id, store_id, dong, used_at) VALUES (?, ?, ?, ?, ?)",
                memberId,
                couponId,
                storeId,
                dong,
                usedAt
        );
    }

    private LocalDateTime parseDateTime(String datetime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(datetime, formatter);
    }

    private record CouponUsageStatsRow(long memberId, String topDong, int topHour) {
    }

}
