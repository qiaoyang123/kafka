package com.ggj.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author: <a href="mailto:qiaoy@gegejia.com">qy</a>
 * @version: 1.0 2018/11/14 16:26
 * @since 1.0
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.ggj.bigdata")
public class ServerBootApplication  extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(ServerBootApplication.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(ServerBootApplication.class);
    }
}
