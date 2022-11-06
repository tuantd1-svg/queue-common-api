package com.example.queuecommonapi.producer;


import com.example.commonapi.model.PageResultMessage;
import com.example.commonapi.model.ResultMessage;
import com.example.queuecommonapi.config.QueueConfig;
import com.example.queuecommonapi.config.Receiver;
import com.example.queuecommonapi.model.Payload;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Repository;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
@Repository
public class QueueProducer implements IQueueProducer {
    @Autowired
    private final Receiver receiver;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public QueueProducer(Receiver receiver) {
        this.receiver = receiver;
    }

    private void usingMqQueue(Payload payloadQueueRDto) {
        try {
            rabbitTemplate.convertAndSend(payloadQueueRDto.getExchange(), payloadQueueRDto.getRoutingKey(), payloadQueueRDto.getPayload());
            receiver.getLatch().await(1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ResultMessage<?> usingMqRPCQueue(Payload payload) {
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        return rabbitTemplate.convertSendAndReceiveAsType(payload.getExchange(), payload.getRoutingKey(), payload.getPayload(),correlationData, ParameterizedTypeReference.forType(ResultMessage.class));
    }

    private PageResultMessage<?> pageUsingMqRPCQueue(Payload payload)
    {
        CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
        return rabbitTemplate.convertSendAndReceiveAsType(payload.getExchange(), payload.getRoutingKey(), payload.getPayload(),correlationData, ParameterizedTypeReference.forType(PageResultMessage.class));
    }
    @Override
    public void blockingStartQueue(String queue, Object payload) {
        Payload payloadQueueRDto = new Payload();
        switch (queue) {
            case QueueConfig.VALIDATE_HANDLE_Q_SHOP_CHANGE_PASS:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.VALIDATE_HANDLE_R_SHOP_CHANGE_PASS);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_MAIL_SEND:
                payloadQueueRDto.setExchange(QueueConfig.E_MAIL_SEND);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_MAIL_SEND);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_AUTHORIZE_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_AUTHORIZE_USER);
                payloadQueueRDto.setPayload(payload);
                break;
        }
       usingMqQueue(payloadQueueRDto);
    }


    @Override
    public ResultMessage blockingStartRPCQueue(String queue, Object payload) {
        Payload payloadQueueRDto =new Payload();
        switch (queue) {
            case QueueConfig.Q_GET_SHOP_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_GET_SHOP_USER);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_CREATE_SHOP_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_CREATE_SHOP_USER);
                payloadQueueRDto.setPayload(payload);
                break;

            case QueueConfig.Q_CREATE_CATEGORY:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_CREATE_CATEGORY);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_CREATE_PRODUCT:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_CREATE_PRODUCT);
                payloadQueueRDto.setPayload(payload);
                break;

        }

        return usingMqRPCQueue(payloadQueueRDto);
    }

    @Override
    public PageResultMessage blockingStartRPCQueuePage(String queue, Object payload) {
        Payload payloadQueueRDto = new Payload();
        switch (queue) {
            case QueueConfig.Q_GET_ORDER_ADMIN:
                payloadQueueRDto.setExchange(QueueConfig.E_SHOP_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_GET_ORDER_ADMIN);
                payloadQueueRDto.setPayload(payload);
                break;
        }
        return pageUsingMqRPCQueue(payloadQueueRDto);
    }
}
