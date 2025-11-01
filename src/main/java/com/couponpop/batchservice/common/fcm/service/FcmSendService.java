package com.couponpop.batchservice.common.fcm.service;

import com.couponpop.batchservice.common.fcm.factory.FcmMessageFactory;
import com.google.firebase.messaging.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class FcmSendService {

    // FCM 멀티캐스트 API는 한 번의 요청에 최대 500개의 토큰만 허용한다.
    private static final int FCM_MULTICAST_LIMIT = 500;

    private final FcmMessageFactory fcmMessageFactory;

    /**
     * 단일 기기 푸시 알림 전송
     *
     * @param memberId 회원 ID
     * @param token    FCM 토큰
     * @param title    알림 제목
     * @param body     알림 내용
     * @throws FirebaseMessagingException FCM 전송 중 오류 발생 시 예외 처리
     */
    public void sendNotification(Long memberId, String token, String title, String body) throws FirebaseMessagingException {
        log.info("단일 기기 푸시 알림 전송 시작: memberId={}, token={}", memberId, token);

        if (!StringUtils.hasText(token)) {
            log.info("FCM 전송 토큰이 유효하지 않아 전송을 건너뜁니다.");
            return;
        }

        String failureReason = null;

        Message message = fcmMessageFactory.createMessage(token, title, body);
        try {
            FirebaseMessaging.getInstance().send(message);
        } catch (FirebaseMessagingException e) {
            log.error("FCM 전송 중 오류 발생: {}", e.getMessage());

            throw e; // 예외 재던지기
        }
    }

    /**
     * 멤버별 다중 기기 푸시 알림 전송
     *
     * @param memberId 멤버 ID
     * @param tokens   FCM 토큰 리스트
     * @param title    알림 제목
     * @param body     알림 내용
     * @throws FirebaseMessagingException FCM 전송 중 오류 발생 시 예외 처리
     */
    public void sendNotification(Long memberId, List<String> tokens, String title, String body) throws FirebaseMessagingException {
        log.info("멤버별 다중 기기 푸시 알림 전송 시작: memberId={}, tokensCount={}", memberId, tokens.size());

        if (tokens == null || tokens.isEmpty()) {
            log.info("FCM 전송 토큰이 비어 있어 전송을 건너뜁니다.");
            return;
        }

        for (int start = 0; start < tokens.size(); start += FCM_MULTICAST_LIMIT) {
            int end = Math.min(start + FCM_MULTICAST_LIMIT, tokens.size());
            List<String> batch = tokens.subList(start, end);

            MulticastMessage message = fcmMessageFactory.createMulticastMessage(batch, title, body);

            log.info("[FCM 다건 전송 요청] tokens={}", batch.size());
            BatchResponse response = FirebaseMessaging.getInstance().sendEachForMulticast(message);
            log.info("[FCM 다건 전송 결과] 성공={} 실패={}", response.getSuccessCount(), response.getFailureCount());
        }
    }
}
