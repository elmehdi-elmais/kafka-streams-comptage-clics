package com.jcore.app.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class ClickController {
    private static final String TOPIC = "clicks";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/click")
    @ResponseBody
    public String click(@RequestParam(defaultValue = "user1") String userId) {
        kafkaTemplate.send(TOPIC, userId, "click");
        System.out.println("Clic envoyé -> clé: " + userId + ", valeur: click");
        return "OK";
    }
}
