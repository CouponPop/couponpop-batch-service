package com.couponpop.batchservice.domain.coupon.dto;

import java.util.List;

public record CouponUsageStatsFcmSendDto(
        Long memberId,
        List<String> fcmTokens,
        String title,
        String body
) {

    public static CouponUsageStatsFcmSendDto of(
            Long memberId,
            List<String> fcmTokens,
            String title,
            String body
    ) {
        return new CouponUsageStatsFcmSendDto(memberId, fcmTokens, title, body);
    }
}
