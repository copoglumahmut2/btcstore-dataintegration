package com.btc_store.dataintegration.controller.interceptor.config;


import com.btc_store.dataintegration.controller.interceptor.ValidationLocaleInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Component
@RequiredArgsConstructor
public class InterceptorConfig implements WebMvcConfigurer {

    private final ApplicationContext context;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(context.getBean(ValidationLocaleInterceptor.class));
    }
}
