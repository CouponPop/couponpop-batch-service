package com.couponpop.batchservice.common.rabbitmq.publisher;

import com.couponpop.couponpopcoremodule.dto.coupon.event.model.CouponUsageStatsFcmSendMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import static com.couponpop.couponpopcoremodule.constants.RabbitMqExchanges.COUPON_EXCHANGE;

@Slf4j
@Service
@RequiredArgsConstructor
public class CouponUsageStatsFcmSendPublisher {

    public static final String COUPON_USAGE_STATS_FCM_SEND_ROUTING_KEY = "coupon.usage.stats.fcm.send";

    private final RabbitTemplate rabbitTemplate;

    public void publish(CouponUsageStatsFcmSendMessage message) {
        try {
            rabbitTemplate.convertAndSend(COUPON_EXCHANGE, COUPON_USAGE_STATS_FCM_SEND_ROUTING_KEY, message);
        } catch (AmqpException e) {
            log.error("쿠폰 사용 통계 FCM 발송 요청 전송에 실패했습니다. message: {}", message, e);
            throw e;
        }
    }
}
