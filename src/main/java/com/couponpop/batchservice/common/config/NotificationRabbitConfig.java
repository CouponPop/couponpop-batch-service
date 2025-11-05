package com.couponpop.batchservice.common.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NotificationRabbitConfig {

    @Value("${rabbitmq.coupon-usage-stats-fcm-send.exchange}")
    private String couponUsageStatsFcmSendExchange;
    @Value("${rabbitmq.coupon-usage-stats-fcm-send.routing-key}")
    private String couponUsageStatsFcmSendRoutingKey;
    @Value("${rabbitmq.coupon-usage-stats-fcm-send.queue}")
    private String couponUsageStatsFcmSendQueue;

    @Bean
    public DirectExchange couponUsageStatsExchange() {
        return ExchangeBuilder.directExchange(couponUsageStatsFcmSendExchange)
                .durable(true)
                .build();
    }

    @Bean
    public Queue couponUsageStatsQueue() {
        return QueueBuilder.durable(couponUsageStatsFcmSendQueue).build();
    }

    @Bean
    public Binding couponUsageStatsBinding(DirectExchange couponUsageStatsExchange, Queue couponUsageStatsQueue) {
        return BindingBuilder.bind(couponUsageStatsQueue)
                .to(couponUsageStatsExchange)
                .with(couponUsageStatsFcmSendRoutingKey);
    }

    @Bean
    public MessageConverter rabbitMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter rabbitMessageConverter) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(rabbitMessageConverter);
        return rabbitTemplate;
    }
}
