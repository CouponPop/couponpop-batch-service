package com.sparta.couponpopbatch.domain.coupon.dto;

import java.time.LocalDate;

public record CouponUsageStatsDto(
        Long memberId,
        String topDong,
        Integer topHour,
        LocalDate aggregatedAt
) {
}
