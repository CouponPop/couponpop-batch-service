package com.couponpop.batchservice.domain.notification.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NotificationTemplates {

    // 쿠폰 수령 알림 템플릿
    public static final String COUPON_USAGE_STATS_TITLE = "%s에 현재 %d개의 쿠폰 이벤트가 진행중입니다";
    public static final String COUPON_USAGE_STATS_BODY = "쿠폰을 가장 많이 사용한 시간대인 %d시에 맞춰 쿠폰 이벤트 소식을 전해드려요.";

}
