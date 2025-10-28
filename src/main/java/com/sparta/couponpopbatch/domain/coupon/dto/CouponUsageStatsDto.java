package com.sparta.couponpopbatch.domain.coupon.dto;

public record CouponUsageStatsDto(
        Long memberId,
        String topDong,
        Integer topHour
) {
}
