package com.sparta.couponpopbatch.domain.coupon.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

import static com.sparta.couponpopbatch.batch.CouponUsageStatsJobConfig.COUPON_USAGE_STATS_JOB;

@RestController
@RequiredArgsConstructor
public class CouponUsageStatsJobController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    @PostMapping("/jobs/coupon-usage-stats")
    public String launchCouponUsageStatsJob(
            @RequestParam LocalDate runDate
    ) throws Exception {
        Job job = jobRegistry.getJob(COUPON_USAGE_STATS_JOB);
        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", runDate)
                .toJobParameters();

        jobLauncher.run(job, jobParameters);
        return "Coupon Usage Stats Job has been launched.";
    }

}
