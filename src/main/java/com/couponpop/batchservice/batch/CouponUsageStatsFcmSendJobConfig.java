package com.couponpop.batchservice.batch;

import com.couponpop.batchservice.common.client.NotificationFeignClient;
import com.couponpop.batchservice.common.client.StoreFeignClient;
import com.couponpop.batchservice.domain.coupon.dto.CouponUsageStatsDto;
import com.couponpop.batchservice.domain.couponevent.repository.CouponEventJdbcRepository;
import com.couponpop.couponpopcoremodule.dto.fcmtoken.response.FcmTokensResponse;
import com.couponpop.couponpopcoremodule.dto.store.response.StoreIdsByDongResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import java.util.Map;
import java.util.stream.Collectors;

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
            ItemWriter<CouponUsageStatsDto> couponUsageStatsFcmSendWriter
    ) {
        return new StepBuilder(COUPON_USAGE_STATS_FCM_SEND_STEP, jobRepository)
                .<CouponUsageStatsDto, CouponUsageStatsDto>chunk(CHUNK_SIZE, txManager)
                .reader(couponUsageStatsFcmSendReader)
                .writer(couponUsageStatsFcmSendWriter)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<CouponUsageStatsDto> couponUsageStatsFcmSendReader(
            @Value("#{jobParameters['runDate'] ?: null}") LocalDate runDateParam,
            @Value("#{jobParameters['targetHour'] ?: null}") Long targetHourParam,
            Clock clock
    ) {
        // 커서 기반 스트리밍으로 대량 데이터를 안정적으로 읽고, 복잡한 최신 통계 조회 SQL을 실행한 결과를
        // 그대로 순차 처리하기 위해 JdbcCursorItemReader를 사용한다. 페이징 방식 대비 커넥션 재생성이나
        // 오프셋 계산 비용이 없어 성능 부담이 적고, 정렬·집계 조건을 유지한 채 chunk 처리 흐름을 단순화할 수 있다.

        log.info("쿠폰 사용 통계 FCM 알림 발송을 위한 데이터 조회를 시작합니다.");

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
    public ItemWriter<CouponUsageStatsDto> couponUsageStatsFcmSendWriter(
            NotificationFeignClient notificationFeignClient,
            StoreFeignClient storeFeignClient,
            @Value("#{jobParameters['runDate'] ?: null}") LocalDate runDateParam,
            @Value("#{jobParameters['targetHour'] ?: null}") Long targetHourParam,
            Clock clock,
            CouponEventJdbcRepository couponEventJdbcRepository
    ) {

        LocalDate referenceDate = runDateParam != null ? runDateParam : LocalDate.now(clock);
        int referenceHour = targetHourParam != null ? targetHourParam.intValue() : LocalDateTime.now(clock).getHour();
        LocalDateTime referenceTime = LocalDateTime.of(referenceDate, LocalTime.of(referenceHour, 0));

        return items -> {
            log.info("쿠폰 사용 통계 FCM 알림 발송 작업을 시작합니다.");

            List<Long> memberIds = items.getItems().stream()
                    .map(CouponUsageStatsDto::memberId)
                    .toList();

            // 회원별 FCM 토큰 조회
            List<FcmTokensResponse> fcmTokensResponses = notificationFeignClient.fetchFcmTokensByMemberIds(memberIds).getData();

            // memberId -> FCM Token List 매핑 생성
            Map<Long, List<String>> memberIdToTokensMap = fcmTokensResponses.stream()
                    .collect(Collectors.toMap(
                            FcmTokensResponse::memberId,
                            FcmTokensResponse::fcmTokens
                    ));

            // topDong별 매장 IDs 조회
            List<String> topDongs = items.getItems().stream()
                    .map(CouponUsageStatsDto::topDong)
                    .distinct()
                    .toList();
            List<StoreIdsByDongResponse> storeIdsByDongResponses = storeFeignClient.fetchStoreIdsByDongs(topDongs).getData();

            // dong -> Store ID List 매핑 생성
            Map<String, List<Long>> dongToStoreIdsMap = storeIdsByDongResponses.stream()
                    .collect(Collectors.toMap(
                            StoreIdsByDongResponse::dong,
                            StoreIdsByDongResponse::storeIds
                    ));

            for (CouponUsageStatsDto item : items) {
                Long memberId = item.memberId();
                String topDong = item.topDong();
                int topHour = item.topHour();

                // 회원의 FCM Token 조회
                List<String> tokens = memberIdToTokensMap.get(memberId);
                if (tokens == null || tokens.isEmpty()) {
                    log.info("회원 {}의 알림은 FCM 토큰이 없어 제외되었습니다.", memberId);
                    continue;
                }

                // topDong에 해당하는 매장 IDs를 IN 쿼리 조건으로 사용하여 진행 중인 쿠폰 이벤트 개수 조회
                List<Long> storeIds = dongToStoreIdsMap.get(topDong);
                int activeEventCount = couponEventJdbcRepository.countActiveCouponEventsByStoreIds(storeIds, referenceTime);
                log.info("회원 {}의 topDong '{}'의 기준 시각 '{}'에 매장 IDs {}에서 진행 중인 쿠폰 이벤트 개수: {}",
                        memberId, topDong, referenceTime, storeIds, activeEventCount);

                if (activeEventCount <= 0) {
                    log.info("회원 {}의 알림은 진행 중인 쿠폰 이벤트가 없어 제외되었습니다.", memberId);
                    continue;
                }

                log.info("알림 발송 시작");
                for (String token : tokens) {
                    // TODO: 알림 발송 (RabbitMQ 메세지 큐로 비동기 전송)
                }
            }
        };
    }
}
