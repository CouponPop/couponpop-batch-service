package com.sparta.couponpopbatch.batch;

import com.sparta.couponpopbatch.domain.coupon.dto.CouponUsageStatsDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CouponUsageStatsJobConfig {

    public static final String COUPON_USAGE_STATS_JOB = "couponUsageStatsJob";
    public static final String COUPON_USAGE_STATS_STEP = "couponUsageStatsStep";
    private static final int CHUNK_SIZE = 1000;
    private static final int STATS_AGGREGATION_DAYS = 20;

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager txManager;

    @Bean
    public Job couponUsageStatsJob(
            @Qualifier("couponUsageStatsStep")
            Step couponUsageStatsStep
    ) {

        log.info("couponUsageStatsJob started");

        return new JobBuilder(COUPON_USAGE_STATS_JOB, jobRepository)
                .start(couponUsageStatsStep)
                .build();
    }

    @Bean
    public Step couponUsageStatsStep(
            @Qualifier("couponUsageStatsReader")
            JdbcCursorItemReader<CouponUsageStatsDto> couponUsageStatsReader,
            @Qualifier("couponUsageStatsWriter")
            JdbcBatchItemWriter<CouponUsageStatsDto> couponUsageStatsWriter
    ) {

        return new StepBuilder(COUPON_USAGE_STATS_STEP, jobRepository)
                .<CouponUsageStatsDto, CouponUsageStatsDto>chunk(CHUNK_SIZE, txManager)
                .reader(couponUsageStatsReader)
                .writer(couponUsageStatsWriter)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<CouponUsageStatsDto> couponUsageStatsReader(
            @Value("#{jobParameters['runDate']}") LocalDate runDateParam
    ) {

        String sql = """
                WITH filtered AS (
                    SELECT
                        ch.member_id,
                        s.dong,
                        ch.created_at,
                        HOUR(ch.created_at) AS usage_hour
                    FROM coupon_histories ch
                        LEFT JOIN stores s ON ch.store_id = s.id
                    WHERE ch.created_at BETWEEN ? AND ?
                        AND ch.coupon_status = 'ISSUED'
                ),
                dong_ranked AS (
                    SELECT
                        member_id,
                        dong,
                        COUNT(*)      AS usage_count,
                        MAX(created_at)  AS recent_used_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY member_id
                            ORDER BY COUNT(*) DESC, MAX(created_at) DESC, dong ASC  -- 동률 3순위 안정화
                        ) AS rn
                    FROM filtered
                    GROUP BY member_id, dong
                ),
                hour_ranked_by_dong AS (
                    SELECT
                        member_id,
                        dong,
                        usage_hour,
                        COUNT(*)      AS usage_count,
                        MAX(created_at)  AS recent_used_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY member_id, dong
                            ORDER BY COUNT(*) DESC, MAX(created_at) DESC, usage_hour ASC  -- 동률 3순위 안정화
                        ) AS rn
                    FROM filtered
                    GROUP BY member_id, dong, usage_hour
                )
                SELECT
                    r.member_id  AS memberId,
                    r.dong       AS topDong,
                    h.usage_hour AS topHour
                FROM dong_ranked r
                JOIN hour_ranked_by_dong h
                      ON h.member_id = r.member_id
                     AND h.dong      = r.dong
                     AND h.rn        = 1
                WHERE r.rn = 1
                ORDER BY r.member_id
                """;

        return new JdbcCursorItemReaderBuilder<CouponUsageStatsDto>()
                .name("couponUsageStatsReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper((rs, i) -> new CouponUsageStatsDto(
                        rs.getLong("memberId"),
                        rs.getString("topDong"),
                        rs.getInt("topHour"),
                        runDateParam
                ))
                .preparedStatementSetter(ps -> {
                    LocalDateTime from = runDateParam.minusDays(STATS_AGGREGATION_DAYS).atStartOfDay(); // Job 실행 20일 전 00:00:00
                    LocalDateTime to = runDateParam.atStartOfDay().plusDays(1).minusSeconds(1); // Job 실행 당일 23:59:59
                    ps.setTimestamp(1, Timestamp.valueOf(from));
                    ps.setTimestamp(2, Timestamp.valueOf(to));
                })
                // MySQL 드라이버가 서버 커서를 흉내 내는 방식 때문에 커서 위치 검증을 시도하면 SQLException 발생
                // MySQL에서는 이 검증이 의미 없고 오히려 실패를 일으킬 수 있어서 비활성화하는 게 안전
                .verifyCursorPosition(false)
                // 한 번에 DB에서 얼마나 많은 로우를 가져올지 결정하는 값
                // MySQL 같은 운영 DB에서는 Integer.MIN_VALUE로 설정하면 서버 커서(스트리밍)를 활성화해 메모리 절약
                .fetchSize(Integer.MIN_VALUE)
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<CouponUsageStatsDto> couponUsageStatsWriter() {

        String sql = """
                    INSERT INTO coupon_usage_stats (member_id, top_dong, top_hour, aggregated_at)
                    VALUES (:memberId, :topDong, :topHour, :aggregatedAt)
                """;

        return new JdbcBatchItemWriterBuilder<CouponUsageStatsDto>()
                .dataSource(dataSource) // 데이터 소스 설정
                .sql(sql) // SQL 쿼리 설정
                .beanMapped() // DTO 필드를 SQL 파라미터에 매핑
                .assertUpdates(true) // 업데이트된 행 수를 확인
                .build();
    }

}
