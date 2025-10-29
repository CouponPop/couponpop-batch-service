package com.sparta.couponpopbatch.batch;

import com.sparta.couponpopbatch.common.fcm.service.FcmSendService;
import com.sparta.couponpopbatch.domain.coupon.dto.CouponUsageStatsDto;
import com.sparta.couponpopbatch.domain.coupon.dto.CouponUsageStatsFcmSendDto;
import com.sparta.couponpopbatch.domain.coupon.repository.CouponUsageStatsFcmSendJdbcRepository;
import com.sparta.couponpopbatch.domain.member.repository.MemberFcmTokenJdbcRepository;
import com.sparta.couponpopbatch.domain.notification.constants.NotificationTemplates;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Date;
import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CouponUsageStatsFcmSendJobConfig {

    public static final String COUPON_USAGE_STATS_FCM_SEND_JOB = "couponUsageStatsFcmSendJob";
    public static final String COUPON_USAGE_STATS_FCM_SEND_STEP = "couponUsageStatsFcmSendStep";

    private static final int CHUNK_SIZE = 1000;
    private static final int DEFAULT_LOOKBACK_DAYS = 2;

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager txManager;

    @Bean
    public Job couponUsageStatsFcmSendJob(Step couponUsageStatsFcmSendStep) {
        return new JobBuilder(COUPON_USAGE_STATS_FCM_SEND_JOB, jobRepository)
                .start(couponUsageStatsFcmSendStep)
                .build();
    }

    @Bean
    public Step couponUsageStatsFcmSendStep(
            JdbcCursorItemReader<CouponUsageStatsDto> couponUsageStatsFcmSendReader,
            ItemProcessor<CouponUsageStatsDto, CouponUsageStatsFcmSendDto> couponUsageStatsFcmSendProcessor,
            ItemWriter<CouponUsageStatsFcmSendDto> couponUsageStatsFcmSendWriter
    ) {
        return new StepBuilder(COUPON_USAGE_STATS_FCM_SEND_STEP, jobRepository)
                .<CouponUsageStatsDto, CouponUsageStatsFcmSendDto>chunk(CHUNK_SIZE, txManager)
                .reader(couponUsageStatsFcmSendReader)
                .processor(couponUsageStatsFcmSendProcessor)
                .writer(couponUsageStatsFcmSendWriter)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<CouponUsageStatsDto> couponUsageStatsFcmSendReader(
            @Value("#{jobParameters['runDate']}") LocalDate runDateParam,
            @Value("#{jobParameters['targetHour']}") Long targetHourParam,
            Clock clock
    ) {
        // 커서 기반 스트리밍으로 대량 데이터를 안정적으로 읽고, 복잡한 최신 통계 조회 SQL을 실행한 결과를
        // 그대로 순차 처리하기 위해 JdbcCursorItemReader를 사용한다. 페이징 방식 대비 커넥션 재생성이나
        // 오프셋 계산 비용이 없어 성능 부담이 적고, 정렬·집계 조건을 유지한 채 chunk 처리 흐름을 단순화할 수 있다.

        String sql = """
                SELECT cus.member_id,
                       cus.top_dong,
                       cus.top_hour,
                       cus.aggregated_at
                FROM coupon_usage_stats cus
                INNER JOIN (
                    SELECT member_id,
                           MAX(aggregated_at) AS latest_aggregated_at
                    FROM coupon_usage_stats
                    WHERE aggregated_at BETWEEN ? AND ?
                      AND top_hour = ?
                    GROUP BY member_id
                ) latest ON latest.member_id = cus.member_id
                         AND latest.latest_aggregated_at = cus.aggregated_at
                WHERE cus.top_hour = ?
                ORDER BY cus.member_id
                """;

        return new JdbcCursorItemReaderBuilder<CouponUsageStatsDto>()
                .name("couponUsageStatsFcmSendReader")
                .dataSource(dataSource)
                .sql(sql)
                .preparedStatementSetter(ps -> {
                    LocalDate endDate = runDateParam != null ? runDateParam : LocalDate.now(clock);
                    LocalDate startDate = endDate.minusDays(DEFAULT_LOOKBACK_DAYS);
                    int targetHour = targetHourParam != null ? targetHourParam.intValue() : LocalDateTime.now(clock).getHour();

                    ps.setDate(1, Date.valueOf(startDate));
                    ps.setDate(2, Date.valueOf(endDate));
                    ps.setInt(3, targetHour);
                    ps.setInt(4, targetHour);
                })
                .rowMapper((rs, rowNum) -> new CouponUsageStatsDto(
                        rs.getLong("member_id"),
                        rs.getString("top_dong"),
                        rs.getInt("top_hour"),
                        rs.getDate("aggregated_at").toLocalDate()
                ))
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<CouponUsageStatsDto, CouponUsageStatsFcmSendDto> couponUsageStatsFcmSendProcessor(
            MemberFcmTokenJdbcRepository memberFcmTokenJdbcRepository,
            CouponUsageStatsFcmSendJdbcRepository couponUsageStatsFcmSendJdbcRepository,
            @Value("#{jobParameters['runDate']}") LocalDate runDateParam,
            @Value("#{jobParameters['targetHour']}") Long targetHourParam,
            Clock clock
    ) {

        return item -> {
            log.info("couponUsageStatsFcmSendProcessor BEGIN");
            log.info("processor item: {}", item);

            // 회원 토큰 조회
            List<String> tokens = memberFcmTokenJdbcRepository.findEnabledTokensByMemberId(item.memberId());
            log.info("회원 {}의 활성화된 FCM 토큰들: {}", item.memberId(), tokens);
            if (tokens.isEmpty()) {
                log.info("회원 {}의 알림은 FCM 토큰이 없어 제외되었습니다.", item.memberId());
                return null;
            }

            LocalDate referenceDate = runDateParam != null ? runDateParam : LocalDate.now(clock);
            int referenceHour = targetHourParam != null ? targetHourParam.intValue() : LocalDateTime.now(clock).getHour();
            LocalDateTime referenceTime = LocalDateTime.of(referenceDate, LocalTime.of(referenceHour, 0));

            // topDong에서 진행 중인 쿠폰 이벤트 개수 조회
            int eventCount = couponUsageStatsFcmSendJdbcRepository.countActiveCouponEventsByDong(item.topDong(), referenceTime);
            log.info("회원 {}의 topDong '{}'에서 기준 시각 '{}'에 진행 중인 쿠폰 이벤트 개수: {}", item.memberId(), item.topDong(), referenceTime, eventCount);
            if (eventCount <= 0) {
                log.info("회원 {}의 알림은 진행 중인 쿠폰 이벤트가 없어 제외되었습니다.", item.memberId());
                return null;
            }

            String title = NotificationTemplates.COUPON_USAGE_STATS_TITLE.formatted(item.topDong(), eventCount);
            String body = NotificationTemplates.COUPON_USAGE_STATS_BODY.formatted(item.topHour());

            return CouponUsageStatsFcmSendDto.of(item.memberId(), tokens, title, body);
        };
    }

    @Bean
    public ItemWriter<CouponUsageStatsFcmSendDto> couponUsageStatsFcmSendWriter(
            FcmSendService fcmSendService
    ) {
        return items -> {
            if (items.isEmpty()) {
                return;
            }

            for (CouponUsageStatsFcmSendDto item : items) {
                log.info("회원 {}에게 FCM 푸시 알림 전송: 제목='{}', 내용='{}', 토큰들={}",
                        item.memberId(), item.title(), item.body(), item.fcmTokens());
                fcmSendService.sendNotification(item.memberId(), item.fcmTokens(), item.title(), item.body());
            }
        };
    }
}
