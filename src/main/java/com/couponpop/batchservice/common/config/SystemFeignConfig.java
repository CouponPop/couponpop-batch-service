package com.couponpop.batchservice.common.config;

import com.couponpop.security.token.SystemTokenProvider;
import feign.RequestInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;

import static com.couponpop.security.constants.SecurityTemplates.BEARER_TOKEN_PREFIX;

/**
 * 시스템 내부 통신을 위한 Feign Client 설정입니다.
 * 모든 요청에 시스템 토큰을 Authorization 헤더에 추가합니다.
 */
@Slf4j
public class SystemFeignConfig {

    /**
     * Feign 요청에 시스템 토큰을 추가하는 RequestInterceptor를 생성합니다.
     *
     * @param systemTokenProvider systemTokenProvider 시스템 토큰을 제공하는 프로바이더
     * @return RequestInterceptor Bean
     */
    @Bean
    public RequestInterceptor systemTokenRequestInterceptor(SystemTokenProvider systemTokenProvider) {
        return requestTemplate -> {
            String systemToken = systemTokenProvider.getToken();
            requestTemplate.header("Authorization", BEARER_TOKEN_PREFIX + systemToken);
        };
    }
}
