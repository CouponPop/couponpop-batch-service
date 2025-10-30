package com.sparta.couponpopbatch.batch;

import com.sparta.couponpopbatch.common.fcm.service.FcmSendService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.jdbc.Sql;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ActiveProfiles("test")
@SpringBatchTest
@SpringBootTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Sql(
        scripts = {
                "/sql/setup_before_coupon_usage_stats_fcm_send_job_test.sql",
                "/sql/insert_dummy_before_coupon_usage_stats_fcm_send_job_test.sql"
        },
        executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD
)
@Sql(
        scripts = "/sql/cleanup_after_coupon_usage_stats_fcm_send_job_test.sql",
        executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD
)
class CouponUsageStatsFcmSendJobConfigTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private final AtomicReference<Instant> nowRef = new AtomicReference<>();

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("couponUsageStatsFcmSendJob")
    private Job couponUsageStatsFcmSendJob;

    @MockitoBean
    private FcmSendService fcmSendService;

    @MockitoBean
    private Clock clock;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(couponUsageStatsFcmSendJob);

        // Clock Default Stubbing
        when(clock.getZone()).thenReturn(KST);
        when(clock.instant()).thenAnswer(invocation -> nowRef.get());

        // 테스트 시작 기본 시간
        setNow(LocalDateTime.of(2025, 10, 25, 10, 0));

        // 호출 횟수 및 인자 검증을 위한 Mockito 초기화
        reset(fcmSendService);
    }

    @Test
    @DisplayName("노량진동 10시가 최다 사용 시간대면 진행 중 쿠폰 이벤트 수만큼 알림을 보낸다.")
    void launchJob_success_whenTopDongIsNoryangjin() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 25, 10, 0));

        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.of(2025, 10, 25))
                .addLong("targetHour", 10L)
                .addString("testId", "noryangjin-10")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> tokensCaptor = ArgumentCaptor.forClass(List.class);
        verify(fcmSendService, times(1)).sendNotification(
                eq(101L),
                tokensCaptor.capture(),
                eq("노량진동에 현재 3개의 쿠폰 이벤트가 진행중입니다"),
                eq("쿠폰을 가장 많이 사용한 시간대인 10시에 맞춰 쿠폰 이벤트 소식을 전해드려요.")
        );
        assertThat(tokensCaptor.getValue()).containsExactlyInAnyOrder("token-101-a", "token-101-b");

        verifyNoMoreInteractions(fcmSendService);
    }

    @Test
    @DisplayName("풍무동 16시가 최다 사용 시간대면 진행 중 쿠폰 이벤트 수만큼 알림을 보낸다.")
    void launchJob_success_whenTopDongIsPungmu() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 26, 16, 0));

        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.of(2025, 10, 26))
                .addLong("targetHour", 16L)
                .addString("testId", "pungmu-16")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> tokensCaptor = ArgumentCaptor.forClass(List.class);
        verify(fcmSendService, times(1)).sendNotification(
                eq(101L),
                tokensCaptor.capture(),
                eq("풍무동에 현재 3개의 쿠폰 이벤트가 진행중입니다"),
                eq("쿠폰을 가장 많이 사용한 시간대인 16시에 맞춰 쿠폰 이벤트 소식을 전해드려요.")
        );
        assertThat(tokensCaptor.getValue()).containsExactlyInAnyOrder("token-101-a", "token-101-b");

        verifyNoMoreInteractions(fcmSendService);
    }

    @Test
    @DisplayName("잡 파라미터가 없으면 Clock 기준 시각으로 진행 중 이벤트를 계산한다.")
    void launchJob_success_whenParametersOmittedUsesClock() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 26, 16, 0));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("testId", "clock-default")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> tokensCaptor = ArgumentCaptor.forClass(List.class);
        verify(fcmSendService, times(1)).sendNotification(
                eq(101L),
                tokensCaptor.capture(),
                eq("풍무동에 현재 3개의 쿠폰 이벤트가 진행중입니다"),
                eq("쿠폰을 가장 많이 사용한 시간대인 16시에 맞춰 쿠폰 이벤트 소식을 전해드려요.")
        );
        assertThat(tokensCaptor.getValue()).containsExactlyInAnyOrder("token-101-a", "token-101-b");

        verifyNoMoreInteractions(fcmSendService);
    }

    private void setNow(LocalDateTime ldtKst) {
        nowRef.set(ldtKst.atZone(KST).toInstant());
    }
}
