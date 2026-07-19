package com.jcore.app.controller;

import com.jcore.app.listener.ClickCountListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ClickCountController {

    private final ClickCountListener listener;

    public ClickCountController(ClickCountListener listener) {
        this.listener = listener;
    }

    // GET /clicks/count -> {"totalClicks": 125}
    @GetMapping("/clicks/count")
    public Map<String, Long> getTotalCount() {
        return Map.of("totalClicks", listener.getTotalClicks());
    }

    // GET /clicks/count/by-user -> {"user1": 45, "user2": 30}
    @GetMapping("/clicks/count/by-user")
    public Map<String, Long> getCountByUser() {
        return listener.getClicksByUser();
    }
}
