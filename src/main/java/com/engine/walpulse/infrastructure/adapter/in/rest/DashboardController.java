package com.engine.walpulse.infrastructure.adapter.in.rest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Controller mapping root and /dashboard routes to the embedded single-page monitoring dashboard.
 */
@Controller
public class DashboardController {

    @GetMapping("/")
    public String index() {
        return "redirect:/dashboard";
    }

    @GetMapping("/dashboard")
    public String dashboard() {
        return "forward:/dashboard/index.html";
    }
}
