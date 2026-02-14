package com.btc_store.dataintegration.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:dataintegration-${spring.profiles.active}.properties")
@PropertySource("classpath:log-${spring.profiles.active}.properties")
@PropertySource("classpath:btc-dataintegration-git.properties")
public class DataIntegrationPropertiesFileConfig {
}
