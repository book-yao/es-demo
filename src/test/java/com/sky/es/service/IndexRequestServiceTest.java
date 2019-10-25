package com.sky.es.service;

import com.sky.es.domain.ElasticEntity;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class IndexRequestServiceTest {

    @Autowired
    private IndexRequestService indexRequestService;

    @Test
    public void testCreateIndex(){
        try {
            indexRequestService.createIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getIndexData(){
        ElasticEntity posts = indexRequestService.getIndexData("posts", "2");

        System.out.println(posts);
    }
}
