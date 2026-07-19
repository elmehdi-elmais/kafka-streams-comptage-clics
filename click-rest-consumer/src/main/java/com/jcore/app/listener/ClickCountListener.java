package com.jcore.app.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ClickCountListener {

    private final AtomicLong totalClicks = new AtomicLong(0);
    private final Map<String, Long> clicksByUser = new ConcurrentHashMap<>();

    @KafkaListener(topics = "click-counts", groupId = "click-rest-consumer-group")
    public void listen(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();
        long count = Long.parseLong(value);

        if ("total".equals(key)) {
            totalClicks.set(count);
        } else {
            clicksByUser.put(key, count);
        }
    }

    public long getTotalClicks() {
        return totalClicks.get();
    }

    public Map<String, Long> getClicksByUser() {
        return clicksByUser;
    }
}