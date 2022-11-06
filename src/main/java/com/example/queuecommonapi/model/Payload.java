package com.example.queuecommonapi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class Payload {
    private String exchange;
    private String routingKey;
    private Object payload;
}
