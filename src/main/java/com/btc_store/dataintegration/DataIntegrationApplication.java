package com.btc_store.dataintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ComponentScan(basePackages = {"com.btc_store.dataintegration", "com.btc_store.service", "com.btc_store.persistence", "com.btc_store.facade"})
@EnableJpaAuditing(auditorAwareRef="auditServiceImpl")
@EnableCaching
@EnableScheduling
@Slf4j
public class DataIntegrationApplication {

    public static void main(String[] args) {
        log.info("Data Integration Application has started");
        SpringApplication.run(DataIntegrationApplication.class, args);
    }
}
