package com.example.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class KafkaController {

    @Autowired
    KafkaTemplate<Object, Object> template;

    @GetMapping(path = "/kafka/{topic}/{msg}")
    public void produceMessage(@PathVariable String topic, @PathVariable String msg) {
        System.out.println("API Hit:" + topic + msg);
        template.send(topic, msg);
    }
}
