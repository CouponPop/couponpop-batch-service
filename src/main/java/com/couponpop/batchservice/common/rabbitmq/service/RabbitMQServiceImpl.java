package com.couponpop.batchservice.common.rabbitmq.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RabbitMQServiceImpl implements RabbitMQService {

    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.coupon-usage-stats-fcm-send.exchange}")
    private String couponUsageStatsFcmSendExchange;
    @Value("${rabbitmq.coupon-usage-stats-fcm-send.routing-key}")
    private String couponUsageStatsFcmSendRoutingKey;

    @Override
    public void sendMessage(Object message) {
        try {
            rabbitTemplate.convertAndSend(
                    couponUsageStatsFcmSendExchange,
                    couponUsageStatsFcmSendRoutingKey,
                    message
            );
        } catch (AmqpException e) {
            log.error("쿠폰 사용 통계 FCM 발송 요청 전송에 실패했습니다. message: {}", message, e);
            throw e;
        }
    }
}
