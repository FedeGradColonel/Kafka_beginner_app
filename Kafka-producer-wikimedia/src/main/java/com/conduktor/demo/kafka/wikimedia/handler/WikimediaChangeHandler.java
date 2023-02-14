package com.conduktor.demo.kafka.wikimedia.handler;

import com.conduktor.demo.kafka.wikimedia.WikimediaChangesProducer;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String > kafkaProducer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String > producer, String topic){
        this.kafkaProducer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading: {}", throwable);
    }
}
