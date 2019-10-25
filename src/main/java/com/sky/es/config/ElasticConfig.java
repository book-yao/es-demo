package com.sky.es.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;

/**
 * es配置类
 * @author JWF
 * @date 2019/10/25
 */
@Slf4j
@Configuration
public class ElasticConfig {

    @Value("${elasticsearch.address:localhost:9200}")
    private String[] address;
    @Value("${elasticsearch.scheme:http}")
    private String scheme;

    @Bean
    public RestClientBuilder restClientBuilder() {
        log.info("elasticsearch's address is ", address);
        HttpHost[] httpHosts = Arrays.stream(address).map(this::createHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        return RestClient.builder(httpHosts);
    }

    @Bean
    public RestHighLevelClient restHighLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        return new RestHighLevelClient(restClientBuilder);
    }

    /**
     * 创建对象
     *
     * @param address eg:http://127.0.0.1:9200
     * @return
     */
    private HttpHost createHttpHost(String address) {
        return HttpHost.create(address);
    }
}
