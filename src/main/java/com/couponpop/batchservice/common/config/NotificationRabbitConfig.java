package com.couponpop.batchservice.common.config;

import com.couponpop.batchservice.common.properties.CouponUsageStatsFcmSendProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class NotificationRabbitConfig {

    private final CouponUsageStatsFcmSendProperties properties;

    @Bean
    public DirectExchange couponUsageStatsExchange() {
        return ExchangeBuilder.directExchange(properties.exchange())
                .durable(true)
                .build();
    }

    @Bean
    public Queue couponUsageStatsQueue() {
        return QueueBuilder.durable(properties.queue()).build();
    }

    @Bean
    public Binding couponUsageStatsBinding(DirectExchange couponUsageStatsExchange, Queue couponUsageStatsQueue) {
        return BindingBuilder.bind(couponUsageStatsQueue)
                .to(couponUsageStatsExchange)
                .with(properties.routingKey());
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
