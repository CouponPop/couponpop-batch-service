package com.couponpop.batchservice.common.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rabbitmq.coupon-usage-stats-fcm-send")
public record CouponUsageStatsFcmSendProperties(
        String exchange,
        String queue,
        String routingKey
) {
}
