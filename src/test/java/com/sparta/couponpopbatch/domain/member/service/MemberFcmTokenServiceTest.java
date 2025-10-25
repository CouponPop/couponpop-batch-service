package com.sparta.couponpopbatch.domain.member.service;

import com.sparta.couponpopbatch.domain.member.repository.MemberFcmTokenJdbcRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

@ExtendWith(MockitoExtension.class)
class MemberFcmTokenServiceTest {

    @Mock
    private MemberFcmTokenJdbcRepository memberFcmTokenJdbcRepository;

    @InjectMocks
    private MemberFcmTokenService memberFcmTokenService;

    @Nested
    @DisplayName("2달 이상 사용하지 않은 FCM 토큰 삭제")
    class DeleteUnusedFcmTokensOlderThanTwoMonths {

        @Test
        @DisplayName("2달 이상 사용하지 않은 FCM 토큰을 삭제한다")
        void deleteUnusedFcmTokensOlderThanTwoMonths_success() {
            // given
            given(memberFcmTokenJdbcRepository.deleteUnusedFcmTokensOlderThanTwoMonths()).willReturn(5);

            // when
            memberFcmTokenService.deleteUnusedFcmTokensOlderThanTwoMonths();

            // then
            then(memberFcmTokenJdbcRepository).should().deleteUnusedFcmTokensOlderThanTwoMonths();
        }

    }

}