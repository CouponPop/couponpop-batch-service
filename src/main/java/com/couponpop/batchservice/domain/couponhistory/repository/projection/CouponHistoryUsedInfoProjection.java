package com.couponpop.batchservice.domain.couponhistory.repository.projection;

import java.time.LocalDateTime;

public record CouponHistoryUsedInfoProjection(
        Long couponHistoryId,
        Long storeId,
        Long memberId,
        LocalDateTime createdAt
) {
}
