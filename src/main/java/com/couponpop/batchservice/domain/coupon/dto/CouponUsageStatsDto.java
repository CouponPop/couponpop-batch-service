package com.couponpop.batchservice.domain.coupon.dto;

import java.time.LocalDate;

public record CouponUsageStatsDto(
        Long memberId,
        String topDong,
        Integer topHour,
        LocalDate aggregatedAt
) {
}
