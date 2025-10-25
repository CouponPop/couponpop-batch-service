package com.sparta.couponpopbatch.domain.member.service;

import com.sparta.couponpopbatch.domain.member.repository.MemberFcmTokenJdbcRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class MemberFcmTokenService {

    private final MemberFcmTokenJdbcRepository memberFcmTokenJdbcRepository;

    @Transactional
    public void deleteUnusedFcmTokensOlderThanTwoMonths() {
        int deletedCount = memberFcmTokenJdbcRepository.deleteUnusedFcmTokensOlderThanTwoMonths();
        log.info("2달 이상 사용하지 않은 FCM 토큰 삭제 완료. 삭제된 토큰 수: {}", deletedCount);
    }

}
