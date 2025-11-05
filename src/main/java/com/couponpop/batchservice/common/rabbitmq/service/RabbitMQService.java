package com.couponpop.batchservice.common.rabbitmq.service;

public interface RabbitMQService {

    void sendMessage(Object message);
}
