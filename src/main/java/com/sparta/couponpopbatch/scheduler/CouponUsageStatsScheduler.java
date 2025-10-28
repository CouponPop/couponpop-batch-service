package com.sparta.couponpopbatch.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

import static com.sparta.couponpopbatch.batch.CouponUsageStatsJobConfig.COUPON_USAGE_STATS_JOB;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponUsageStatsScheduler {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    @Scheduled(cron = "0 0 1 * * *")
    public void runCouponUsageStatsJob() throws Exception {
        try {
            Job job = jobRegistry.getJob(COUPON_USAGE_STATS_JOB);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("runDate", LocalDate.now())
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);
        } catch (Exception e) {
            // 스케줄링 작업 실패 시 로그 기록
            log.error("쿠폰 사용 이력 집계 배치 스케줄링 작업이 실패했습니다: {}", e.getMessage(), e);
        }
    }
}
