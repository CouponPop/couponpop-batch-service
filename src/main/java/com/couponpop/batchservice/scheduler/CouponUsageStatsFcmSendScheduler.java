package com.couponpop.batchservice.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.couponpop.batchservice.batch.CouponUsageStatsFcmSendJobConfig.COUPON_USAGE_STATS_FCM_SEND_JOB;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponUsageStatsFcmSendScheduler {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;
    private final Clock clock;

    // 정각마다 실행
    @Scheduled(cron = "0 0 * * * *")
    public void runCouponUsageStatsFcmSendJob() {
        try {
            LocalDate nowDate = LocalDate.now(clock);
            long targetHour = LocalDateTime.now(clock).getHour();

            Job job = jobRegistry.getJob(COUPON_USAGE_STATS_FCM_SEND_JOB);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("runDate", nowDate)
                    .addLong("targetHour", targetHour)
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException e) {
            log.warn("쿠폰 사용 통계 기반 FCM 알림 발송 배치가 이미 실행 중이거나 재시작 불가한 배치입니다: {}", e.getMessage());
        } catch (Exception e) {
            // 스케줄링 작업 실패 시 로그 기록
            log.error("쿠폰 사용 통계 기반 FCM 알림 발송 배치 스케줄링 작업이 실패했습니다: {}", e.getMessage(), e);
        }
    }
}
