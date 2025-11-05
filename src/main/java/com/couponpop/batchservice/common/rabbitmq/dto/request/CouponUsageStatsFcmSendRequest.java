package com.couponpop.batchservice.common.rabbitmq.dto.request;

import java.util.List;

public record CouponUsageStatsFcmSendRequest(
        Long memberId,
        List<String> tokens,
        String topDong,
        int topHour,
        int activeEventCount
) {

    public static CouponUsageStatsFcmSendRequest of(Long memberId, List<String> tokens, String topDong, int topHour, int activeEventCount) {
        return new CouponUsageStatsFcmSendRequest(memberId, tokens, topDong, topHour, activeEventCount);
    }
}
