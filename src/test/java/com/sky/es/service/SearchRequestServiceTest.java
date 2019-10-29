package com.sky.es.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class SearchRequestServiceTest {
    @Autowired
    private SearchRequestService searchRequestService;

    @Test
    public void testSearch() throws IOException {
        searchRequestService.search();
    }

    @Test
    public void testSearchQuery() throws IOException {
        searchRequestService.searchQuery();
    }

    @Test
    public void scorllRequest() throws IOException {
        searchRequestService.scorllRequest();

    }

}
