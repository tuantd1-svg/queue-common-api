package com.example.queuecommonapi.producer;


import com.example.commonapi.model.ResultMessage;

public interface IQueueProducer<T> {
    void blockingStartQueue(String queue, T payload);
    ResultMessage blockingStartRPCQueue(String queue, T payload);

}
