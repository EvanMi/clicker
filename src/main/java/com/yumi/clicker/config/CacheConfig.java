package com.yumi.clicker.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.yumi.clicker.enums.ClickTypeEnum;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class CacheConfig {
    @Bean
    public Cache<String, ClickTypeEnum> userClickRecordsCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .initialCapacity(100)
                .maximumSize(1000)
                .build();
    }



    @Bean
    public Cache<Long, Long> clickSummaryCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .initialCapacity(100)
                .maximumSize(2000)
                .build();
    }
}
