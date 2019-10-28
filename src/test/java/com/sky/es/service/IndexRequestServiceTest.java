package com.sky.es.service;

import com.sky.es.domain.ElasticEntity;
import org.junit.Assert;
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
        ElasticEntity posts = indexRequestService.getIndexData("posts", "1");

        System.out.println(posts);
    }

    @Test
    public void existsData(){
        boolean posts = indexRequestService.existsData("posts", "1");
        Assert.assertTrue(posts);
    }

    @Test
    public void existsAsyncData(){
        indexRequestService.existsAsyncData("posts", "1");
    }

    @Test
    public void deleteData() throws IOException {
        indexRequestService.deleteData("posts","2");
    }

    @Test
    public void updateData() throws IOException {
        indexRequestService.updateData("posts","5");
    }
}
