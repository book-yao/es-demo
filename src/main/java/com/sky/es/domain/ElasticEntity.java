package com.sky.es.domain;

import lombok.Data;
/**
 * es 实体类
 * @author JWF
 * @date 2019/10/25
 */
@Data
public class ElasticEntity<T> {
    private String index;
    private String id;
    private T data;
}
