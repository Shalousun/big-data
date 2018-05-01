package com.sunyu.hdfs.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(HdfsProperties.class)
public class HdfsConfig {

    @Autowired
    private HdfsProperties hdfsProperties;
}
