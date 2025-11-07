package com.couponpop.batchservice.common.config;

import com.couponpop.security.token.SystemTokenProvider;
import feign.RequestInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.couponpop.security.constants.SecurityTemplates.BEARER_TOKEN_PREFIX;

@Slf4j
@Configuration
public class SystemFeignConfig {

    @Bean
    public RequestInterceptor systemTokenRequestInterceptor(SystemTokenProvider systemTokenProvider) {
        return requestTemplate -> {
            String systemToken = systemTokenProvider.getToken();
            requestTemplate.header("Authorization", BEARER_TOKEN_PREFIX + systemToken);
        };
    }
}
