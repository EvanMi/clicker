package com.yumi.clicker.config;

import com.baomidou.mybatisplus.autoconfigure.ConfigurationCustomizer;
import com.yumi.clicker.enums.ClickTypeEnum;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyBatisPlusConfig {


    @Bean
    public ConfigurationCustomizer codeDescConfigurationCustomizer() {
        return configuration -> {
            configuration.getTypeHandlerRegistry().register(ClickTypeEnum.class, EnumCodeTypeHandler.class);
        };
    }
}
