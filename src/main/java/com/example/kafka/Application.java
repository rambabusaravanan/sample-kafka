package com.example.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
//import org.springframework.kafka.support.converter.RecordMessageConverter;
//import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory,
            KafkaTemplate<Object, Object> template) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(
                new DeadLetterPublishingRecoverer(template), 3)); // dead-letter after 3 tries
        return factory;
    }

//    @Bean
//    public RecordMessageConverter converter() {
//        return new StringJsonMessageConverter();
//    }

    @KafkaListener(id = "RCB1", topics = "RCB")
    public void listenRCB1(String msg) {
        System.out.println("KAFKA Consumer 1 'RCB':" + msg);
    }

    @KafkaListener(id = "CSK1", topics = "CSK")
    public void listenCSK1(String msg) {
        System.out.println("KAFKA Consumer 1 'CSK':" + msg);
    }

    @KafkaListener(id = "CSK2", topics = "CSK")
    public void listenCSK2(String msg) {
        System.out.println("KAFKA Consumer 2 'CSK':" + msg);
    }

    @Bean
    public NewTopic topicCSK() {
        return new NewTopic("CSK",1,(short) 1);
    }

    @Bean
    public NewTopic topicRCB() {
        return new NewTopic("RCB",1,(short) 1);
    }
}
