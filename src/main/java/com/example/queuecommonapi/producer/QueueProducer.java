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
            case QueueConfig.Q_NOTIFICATION_SEND:
                payloadQueueRDto.setExchange(QueueConfig.E_NOTIFICATION_SEND);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_NOTIFICATION_SEND);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_AUTHORIZE:
                payloadQueueRDto.setExchange(QueueConfig.E_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_AUTHORIZE);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_RESET_PASSWORD:
                payloadQueueRDto.setPayload(payload);
                payloadQueueRDto.setExchange(QueueConfig.E_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_RESET_PASSWORD);
                break;
        }
       usingMqQueue(payloadQueueRDto);
    }


    @Override
    public ResultMessage<?> blockingStartRPCQueue(String queue, Object payload) {
        Payload payloadQueueRDto =new Payload();
        switch (queue) {
            case QueueConfig.Q_GET_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_GET_USER);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_CREATE_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_CREATE_USER);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_INIT_RESOURCE:
                payloadQueueRDto.setExchange(QueueConfig.E_RESOURCE);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_INIT_RESOURCE);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_PARAMETER:
                payloadQueueRDto.setExchange(QueueConfig.E_PARAMETER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_PARAMETER);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_VERIFY_RESOURCE:
                payloadQueueRDto.setExchange(QueueConfig.E_RESOURCE);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_VERIFY_RESOURCE);
                payloadQueueRDto.setPayload(payload);
                break;
            case QueueConfig.Q_CORE_CREATE_USER:
                payloadQueueRDto.setExchange(QueueConfig.E_CORE_USER);
                payloadQueueRDto.setRoutingKey(QueueConfig.R_CORE_CREATE_USER);
                payloadQueueRDto.setPayload(payload);
                break;

        }

        return usingMqRPCQueue(payloadQueueRDto);
    }

}
