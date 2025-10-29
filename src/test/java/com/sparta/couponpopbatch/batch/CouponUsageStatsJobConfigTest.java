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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ActiveProfiles("test")
@SpringBatchTest
@SpringBootTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(
        scripts = {
                "/sql/setup_before_coupon_usage_stats_job_test.sql",
                "/sql/insert_dummy_before_coupon_usage_stats_job_test.sql"
        },
        executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
)
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
        LocalDate runDateParam = LocalDate.of(2025, 10, 31);
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", runDateParam) // 2025년 10월 데이터까지 집계
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
                        rs.getInt("top_hour"),
                        runDateParam
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

    private record CouponUsageStatsRow(long memberId, String topDong, int topHour, LocalDate aggregatedAt) {
    }

}
