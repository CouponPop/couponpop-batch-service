package com.couponpop.batchservice.domain.coupon.controller;

import com.couponpop.batchservice.common.exception.CommonErrorCode;
import com.couponpop.batchservice.common.exception.GlobalException;
import com.couponpop.security.annotation.CurrentMember;
import com.couponpop.security.dto.AuthMember;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

import static com.couponpop.batchservice.batch.CouponUsageStatsFcmSendJobConfig.COUPON_USAGE_STATS_FCM_SEND_JOB;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CouponUsageStatsFcmSendJobController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    @PostMapping("/v1/jobs/coupon-usage-stats-fcm-send")
    public ResponseEntity<String> launchCouponUsageStatsFcmSendJob(
            @CurrentMember AuthMember authMember,
            @RequestParam LocalDate runDate,
            @RequestParam Long targetHour
    ) {

        String memberType = authMember.memberType();
        if (memberType == null || (!"ADMIN".equalsIgnoreCase(memberType) && !"ROLE_ADMIN".equalsIgnoreCase(memberType))) {
            throw new GlobalException(CommonErrorCode.ACCESS_DENIED);
        }

        try {
            Job job = jobRegistry.getJob(COUPON_USAGE_STATS_FCM_SEND_JOB);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("runDate", runDate)
                    .addLong("targetHour", targetHour)
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);

            return ResponseEntity.ok().body("쿠폰 사용 통계 기반 FCM 알림 발송 배치 작업이 성공적으로 시작되었습니다.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("쿠폰 사용 통계 기반 FCM 알림 발송 배치 작업이 실패했습니다: " + e.getMessage());
        }
    }
}
