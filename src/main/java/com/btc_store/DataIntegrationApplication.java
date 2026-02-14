package com.btc_store;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
//@EnableJpaAuditing(auditorAwareRef="auditServiceImpl")
@EnableCaching
@EnableScheduling
@Slf4j
public class DataIntegrationApplication {

    public static void main(String[] args) {
        log.info("Data Integration Application has started");
        SpringApplication.run(DataIntegrationApplication.class, args);
    }
}
