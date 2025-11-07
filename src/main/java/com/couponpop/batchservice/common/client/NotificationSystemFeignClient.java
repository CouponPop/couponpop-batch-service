package com.couponpop.batchservice.common.client;

import com.couponpop.batchservice.common.config.SystemFeignConfig;
import com.couponpop.batchservice.common.response.ApiResponse;
import com.couponpop.couponpopcoremodule.dto.fcmtoken.response.FcmTokensResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

@FeignClient(name = "${client.notification-service.name}", url = "${client.notification-service.url}", configuration = SystemFeignConfig.class)
public interface NotificationSystemFeignClient {

    @PostMapping("/internal/v1/fcm-tokens/search")
    ApiResponse<List<FcmTokensResponse>> fetchFcmTokensByMemberIds(@RequestBody List<Long> memberIds);
}
