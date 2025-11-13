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
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

import static com.couponpop.batchservice.batch.CouponUsageStatsJobConfig.COUPON_USAGE_STATS_JOB;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CouponUsageStatsJobController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    @Secured("ROLE_ADMIN")
    @PostMapping("/v1/jobs/coupon-usage-stats")
    public ResponseEntity<String> launchCouponUsageStatsJob(
            @CurrentMember AuthMember authMember,
            @RequestParam LocalDate runDate
    ) {

        String memberType = authMember.memberType();
        if (memberType == null || (!"ADMIN".equalsIgnoreCase(memberType) && !"ROLE_ADMIN".equalsIgnoreCase(memberType))) {
            throw new GlobalException(CommonErrorCode.ACCESS_DENIED);
        }

        try {
            Job job = jobRegistry.getJob(COUPON_USAGE_STATS_JOB);
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLocalDate("runDate", runDate)
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);

            return ResponseEntity.ok().body("쿠폰 사용 통계 집계 배치 작업이 성공적으로 시작되었습니다.");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("쿠폰 사용 통계 집계 배치 작업이 실패했습니다: " + e.getMessage());
        }
    }

}
