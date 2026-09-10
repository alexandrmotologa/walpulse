package com.engine.walpulse;

import com.engine.walpulse.application.dto.WalPulseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(WalPulseProperties.class)
public class WalPulseApplication {

    public static void main(String[] args) {
        SpringApplication.run(WalPulseApplication.class, args);
    }
}
