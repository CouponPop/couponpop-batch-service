package com.couponpop.batchservice.batch;

import com.couponpop.batchservice.common.client.StoreSystemFeignClient;
import com.couponpop.batchservice.domain.coupon.dto.CouponUsageStatsDto;
import com.couponpop.batchservice.domain.coupon.enums.CouponStatus;
import com.couponpop.batchservice.domain.couponhistory.repository.CouponHistoryJdbcRepository;
import com.couponpop.batchservice.domain.couponhistory.repository.projection.CouponHistoryUsedInfoProjection;
import com.couponpop.couponpopcoremodule.dto.store.response.StoreRegionInfoResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CouponUsageStatsJobConfig {

    public static final String COUPON_USAGE_STATS_JOB = "couponUsageStatsJob";
    public static final String COUPON_USAGE_STATS_STEP = "couponUsageStatsStep";
    private static final int CHUNK_SIZE = 1000;
    private static final int STORE_FEIGN_CHUNK_SIZE = 200;
    private static final int STATS_AGGREGATION_DAYS = 20;
    private static final int COUPON_USAGE_COUNT_THRESHOLD = 5;

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager txManager;

    @Bean
    public Job couponUsageStatsJob(
            @Qualifier("couponUsageStatsStep")
            Step couponUsageStatsStep
    ) {

        log.info("{} started", COUPON_USAGE_STATS_JOB);

        return new JobBuilder(COUPON_USAGE_STATS_JOB, jobRepository)
                .start(couponUsageStatsStep)
                .build();
    }

    @Bean
    public Step couponUsageStatsStep(
            @Qualifier("couponUsageStatsReader")
            ListItemReader<CouponUsageStatsDto> couponUsageStatsReader,
            @Qualifier("couponUsageStatsWriter")
            JdbcBatchItemWriter<CouponUsageStatsDto> couponUsageStatsWriter
    ) {

        return new StepBuilder(COUPON_USAGE_STATS_STEP, jobRepository)
                .<CouponUsageStatsDto, CouponUsageStatsDto>chunk(CHUNK_SIZE, txManager)
                .reader(couponUsageStatsReader)
                .writer(couponUsageStatsWriter)
                .build();
    }

    private List<StoreRegionInfoResponse> fetchStoresRegionChunked(StoreSystemFeignClient storeSystemFeignClient, List<Long> storeIds) {

        if (storeIds.isEmpty()) {
            return List.of();
        }

        int chunkCount = (int) Math.ceil((double) storeIds.size() / STORE_FEIGN_CHUNK_SIZE);

        return IntStream.range(0, chunkCount)
                .mapToObj(index -> {
                    int from = index * STORE_FEIGN_CHUNK_SIZE;
                    int to = Math.min(storeIds.size(), from + STORE_FEIGN_CHUNK_SIZE);
                    List<Long> chunk = storeIds.subList(from, to);
                    return storeSystemFeignClient.fetchStoresRegionByIds(chunk).getData();
                })
                .flatMap(List::stream)
                .toList();
    }

    @Bean
    @StepScope
    public ListItemReader<CouponUsageStatsDto> couponUsageStatsReader(
            CouponHistoryJdbcRepository couponHistoryJdbcRepository,
            StoreSystemFeignClient storeSystemFeignClient,
            @Value("#{jobParameters['runDate']}") LocalDate runDateParam
    ) {

        LocalDateTime from = runDateParam.minusDays(STATS_AGGREGATION_DAYS).atStartOfDay(); // Job 실행 20일 전 00:00:00
        LocalDateTime to = runDateParam.atStartOfDay().plusDays(1).minusSeconds(1); // Job 실행 당일 23:59:59

        List<CouponHistoryUsedInfoProjection> couponHistoriesUsedInfos = couponHistoryJdbcRepository.findCouponHistoriesUsedInfo(CouponStatus.USED, from, to);
        List<Long> storeIds = couponHistoriesUsedInfos.stream()
                .map(CouponHistoryUsedInfoProjection::storeId)
                .distinct()
                .toList();

        List<StoreRegionInfoResponse> storeRegionInfoResponses = fetchStoresRegionChunked(storeSystemFeignClient, storeIds);
        Map<Long, String> storeDongMap = storeRegionInfoResponses.stream()
                .collect(Collectors.toMap(
                        StoreRegionInfoResponse::storeId,
                        StoreRegionInfoResponse::dong,
                        (latest, ignored) -> latest // 중복 키 충돌 시 최신 값 유지
                ));

        List<MemberUsage> filtered = couponHistoriesUsedInfos.stream()
                .map(history -> new MemberUsage(
                        history.memberId(),
                        storeDongMap.get(history.storeId()),
                        history.createdAt(),
                        history.createdAt().getHour()
                ))
                .filter(usage -> usage.dong() != null) // 동 정보 없는 데이터는 제외
                .toList();

        Map<Long, Long> memberUsageCounts = filtered.stream()
                .collect(Collectors.groupingBy(
                        MemberUsage::memberId,
                        Collectors.counting()
                ));

        Set<Long> eligibleMembers = memberUsageCounts.entrySet().stream()
                .filter(entry -> entry.getValue() >= COUPON_USAGE_COUNT_THRESHOLD)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        Comparator<MemberUsageAggregation> dongComparator = Comparator
                .comparingLong(MemberUsageAggregation::usageCount)
                .thenComparing(MemberUsageAggregation::recentUsedAt)
                .thenComparing(MemberUsageAggregation::dong);

        Comparator<MemberUsageAggregation> hourComparator = Comparator
                .comparingLong(MemberUsageAggregation::usageCount)
                .thenComparing(MemberUsageAggregation::recentUsedAt)
                .thenComparingInt(MemberUsageAggregation::usageHour);

        List<CouponUsageStatsDto> results = filtered.stream()
                .filter(usage -> eligibleMembers.contains(usage.memberId()))
                .collect(Collectors.groupingBy(MemberUsage::memberId))
                .entrySet()
                .stream()
                .map(entry -> {
                    Long memberId = entry.getKey();
                    List<MemberUsage> usages = entry.getValue();

                    MemberUsageAggregation topDong = usages.stream()
                            .collect(Collectors.groupingBy(MemberUsage::dong))
                            .entrySet()
                            .stream()
                            .map(dongEntry -> new MemberUsageAggregation(
                                    memberId,
                                    dongEntry.getKey(),
                                    null,
                                    dongEntry.getValue().size(),
                                    dongEntry.getValue().stream()
                                            .map(MemberUsage::usedAt)
                                            .max(LocalDateTime::compareTo)
                                            .orElse(LocalDateTime.MIN)
                            ))
                            .max(dongComparator)
                            .orElseThrow();

                    int topHour = usages.stream()
                            .filter(usage -> usage.dong().equals(topDong.dong()))
                            .collect(Collectors.groupingBy(MemberUsage::usageHour))
                            .entrySet()
                            .stream()
                            .map(hourEntry -> new MemberUsageAggregation(
                                    memberId,
                                    topDong.dong(),
                                    hourEntry.getKey(),
                                    hourEntry.getValue().size(),
                                    hourEntry.getValue().stream()
                                            .map(MemberUsage::usedAt)
                                            .max(LocalDateTime::compareTo)
                                            .orElse(LocalDateTime.MIN)
                            ))
                            .max(hourComparator)
                            .orElseThrow()
                            .usageHour;

                    return new CouponUsageStatsDto(memberId, topDong.dong(), topHour, runDateParam);
                })
                .sorted(Comparator.comparing(CouponUsageStatsDto::memberId))
                .toList();

        return new ListItemReader<>(results);
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

    private record MemberUsage(Long memberId, String dong, LocalDateTime usedAt, int usageHour) {
    }

    private record MemberUsageAggregation(
            Long memberId,
            String dong,
            Integer usageHour,
            long usageCount,
            LocalDateTime recentUsedAt
    ) {
    }

}
