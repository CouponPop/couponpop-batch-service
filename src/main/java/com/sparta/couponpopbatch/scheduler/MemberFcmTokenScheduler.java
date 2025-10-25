package com.sparta.couponpopbatch.scheduler;

import com.sparta.couponpopbatch.domain.member.service.MemberFcmTokenService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MemberFcmTokenScheduler {

    private final MemberFcmTokenService memberFcmTokenService;

    /**
     * <h2>2달 이상 사용하지 않은 FCM 토큰 삭제 스케줄러</h2>
     * <p>- 매일 01:00 실행</p>
     */
    @Scheduled(cron = "0 0 1 * * *")
    public void deleteOldFcmTokens() {
        memberFcmTokenService.deleteUnusedFcmTokensOlderThanTwoMonths();
    }

}
