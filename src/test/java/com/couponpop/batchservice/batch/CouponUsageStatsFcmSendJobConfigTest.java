package com.couponpop.batchservice.batch;

import com.couponpop.batchservice.common.client.NotificationFeignClient;
import com.couponpop.batchservice.common.client.StoreFeignClient;
import com.couponpop.batchservice.common.rabbitmq.dto.request.CouponUsageStatsFcmSendRequest;
import com.couponpop.batchservice.common.rabbitmq.publisher.CouponUsageStatsFcmSendPublisher;
import com.couponpop.batchservice.common.response.ApiResponse;
import com.couponpop.couponpopcoremodule.dto.fcmtoken.response.FcmTokensResponse;
import com.couponpop.couponpopcoremodule.dto.store.response.StoreIdsByDongResponse;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
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
    private NotificationFeignClient notificationFeignClient;

    @MockitoBean
    private StoreFeignClient storeFeignClient;

    @MockitoBean
    private CouponUsageStatsFcmSendPublisher couponUsageStatsFcmSendPublisher;

    @MockitoBean
    private Clock clock;

    @BeforeEach
    void setUp() {
        jobLauncherTestUtils.setJob(couponUsageStatsFcmSendJob);

        reset(notificationFeignClient, storeFeignClient, couponUsageStatsFcmSendPublisher, clock);

        when(clock.getZone()).thenReturn(KST);
        when(clock.instant()).thenAnswer(invocation -> nowRef.get());

        setNow(LocalDateTime.of(2025, 10, 25, 10, 0));
    }

    @Test
    @DisplayName("노량진동 10시가 최다 사용 시간대면 진행 중 이벤트 수만큼 메시지를 RabbitMQ로 전송한다.")
    void launchJob_success_whenTopDongIsNoryangjin() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 25, 10, 0));
        mockNotificationFeignResponse(101L, List.of("token-101-a", "token-101-b"));
        mockStoreFeignResponse(Map.of("노량진동", List.of(1L)));

        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.of(2025, 10, 25))
                .addLong("targetHour", 10L)
                .addString("testId", "noryangjin-10")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        // RabbitMQ로 전송된 메시지를 모두 모아 토큰별 발송이 이뤄졌는지 검증한다.
        ArgumentCaptor<CouponUsageStatsFcmSendRequest> messageCaptor = ArgumentCaptor.forClass(CouponUsageStatsFcmSendRequest.class);
        verify(couponUsageStatsFcmSendPublisher, times(1)).publish(messageCaptor.capture());

        CouponUsageStatsFcmSendRequest request = messageCaptor.getValue();
        assertThat(request.memberId()).isEqualTo(101L);
        assertThat(request.tokens()).containsExactlyInAnyOrder("token-101-a", "token-101-b");
        assertThat(request.topDong()).isEqualTo("노량진동");
        assertThat(request.topHour()).isEqualTo(10);
        assertThat(request.activeEventCount()).isEqualTo(3);

        verify(notificationFeignClient).fetchFcmTokensByMemberIds(eq(List.of(101L)));
        verify(storeFeignClient).fetchStoreIdsByDongs(eq(List.of("노량진동")));
        verifyNoMoreInteractions(notificationFeignClient, storeFeignClient, couponUsageStatsFcmSendPublisher);
    }

    @Test
    @DisplayName("풍무동 16시가 최다 사용 시간대면 진행 중 이벤트 수만큼 메시지를 RabbitMQ로 전송한다.")
    void launchJob_success_whenTopDongIsPungmu() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 26, 16, 0));
        mockNotificationFeignResponse(101L, List.of("token-101-a", "token-101-b"));
        mockStoreFeignResponse(Map.of("풍무동", List.of(2L)));

        JobParameters jobParameters = new JobParametersBuilder()
                .addLocalDate("runDate", LocalDate.of(2025, 10, 26))
                .addLong("targetHour", 16L)
                .addString("testId", "pungmu-16")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        ArgumentCaptor<CouponUsageStatsFcmSendRequest> messageCaptor = ArgumentCaptor.forClass(CouponUsageStatsFcmSendRequest.class);
        verify(couponUsageStatsFcmSendPublisher, times(1)).publish(messageCaptor.capture());

        CouponUsageStatsFcmSendRequest request = messageCaptor.getValue();
        assertThat(request.memberId()).isEqualTo(101L);
        assertThat(request.tokens()).containsExactlyInAnyOrder("token-101-a", "token-101-b");
        assertThat(request.topDong()).isEqualTo("풍무동");
        assertThat(request.topHour()).isEqualTo(16);
        assertThat(request.activeEventCount()).isEqualTo(3);

        verify(notificationFeignClient).fetchFcmTokensByMemberIds(eq(List.of(101L)));
        verify(storeFeignClient).fetchStoreIdsByDongs(eq(List.of("풍무동")));
        verifyNoMoreInteractions(notificationFeignClient, storeFeignClient, couponUsageStatsFcmSendPublisher);
    }

    @Test
    @DisplayName("잡 파라미터가 없으면 Clock 기준 시각으로 진행 중 이벤트를 계산한 뒤 메시지를 RabbitMQ로 전송한다.")
    void launchJob_success_whenParametersOmittedUsesClock() throws Exception {
        // given
        setNow(LocalDateTime.of(2025, 10, 26, 16, 0));
        mockNotificationFeignResponse(101L, List.of("token-101-a", "token-101-b"));
        mockStoreFeignResponse(Map.of("풍무동", List.of(2L)));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("testId", "clock-default")
                .toJobParameters();

        // when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        // then
        assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        ArgumentCaptor<CouponUsageStatsFcmSendRequest> messageCaptor = ArgumentCaptor.forClass(CouponUsageStatsFcmSendRequest.class);
        verify(couponUsageStatsFcmSendPublisher, times(1)).publish(messageCaptor.capture());

        CouponUsageStatsFcmSendRequest request = messageCaptor.getValue();
        assertThat(request.memberId()).isEqualTo(101L);
        assertThat(request.tokens()).containsExactlyInAnyOrder("token-101-a", "token-101-b");
        assertThat(request.topDong()).isEqualTo("풍무동");
        assertThat(request.topHour()).isEqualTo(16);
        assertThat(request.activeEventCount()).isEqualTo(3);

        verify(notificationFeignClient).fetchFcmTokensByMemberIds(eq(List.of(101L)));
        verify(storeFeignClient).fetchStoreIdsByDongs(eq(List.of("풍무동")));
        verifyNoMoreInteractions(notificationFeignClient, storeFeignClient, couponUsageStatsFcmSendPublisher);
    }

    private void setNow(LocalDateTime ldtKst) {
        nowRef.set(ldtKst.atZone(KST).toInstant());
    }

    private void mockNotificationFeignResponse(Long memberId, List<String> tokens) {
        @SuppressWarnings("unchecked")
        // Notification 서비스에서 내려주는 응답 DTO 형태 그대로 구성해 잡 로직의 매핑을 검증한다.
        ApiResponse<List<FcmTokensResponse>> apiResponse = mock(ApiResponse.class);

        FcmTokensResponse tokensResponse = new FcmTokensResponse(memberId, tokens);
        when(apiResponse.getData()).thenReturn(List.of(tokensResponse));
        when(notificationFeignClient.fetchFcmTokensByMemberIds(anyList())).thenReturn(apiResponse);
    }

    private void mockStoreFeignResponse(Map<String, List<Long>> storeIdsByDong) {
        when(storeFeignClient.fetchStoreIdsByDongs(anyList())).thenAnswer(invocation -> {
            List<String> requestedDongs = invocation.getArgument(0);
            // 요청된 동마다 매장 ID 리스트를 반환해 실제 Feign 응답과 동일한 구조를 재현한다.
            List<StoreIdsByDongResponse> responses = requestedDongs.stream()
                    .map(dong -> new StoreIdsByDongResponse(dong, storeIdsByDong.getOrDefault(dong, List.of())))
                    .toList();

            @SuppressWarnings("unchecked")
            ApiResponse<List<StoreIdsByDongResponse>> apiResponse = mock(ApiResponse.class);
            when(apiResponse.getData()).thenReturn(responses);
            return apiResponse;
        });
    }
}
