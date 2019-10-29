package com.sky.es.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 搜索API
 *
 * @author JWF
 * @date 2019/10/29
 */
@Service
@Slf4j
public class SearchRequestService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public void search() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = search.getHits();
        TotalHits totalHits = hits.getTotalHits();
        log.info("total value :{},relation:{}",totalHits.value,totalHits.relation);
        SearchHit[] hits1 = hits.getHits();
        for(SearchHit searchHit: hits1){

            String id = searchHit.getId();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
        }

    }

    public void searchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("bank");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.termQuery("user","jwf"));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.ASC));
        TermsAggregationBuilder group_by_state = AggregationBuilders.terms("group_by_state").field("state.keyword");
        group_by_state.subAggregation(AggregationBuilders.avg("avg_balance").field("balance"));
        searchSourceBuilder.aggregation(group_by_state);
        // 关闭检索，获取不到结果
//        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = search.getHits();
        TotalHits totalHits = hits.getTotalHits();
        log.info("total value :{},relation:{}",totalHits.value,totalHits.relation);
        SearchHit[] hits1 = hits.getHits();
        for(SearchHit searchHit: hits1){

            String id = searchHit.getId();
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            System.out.println(sourceAsMap);
        }

    }

    public void scorllRequest() throws IOException {
        Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest("bank");
        searchRequest.scroll(scroll);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("age",34));
        searchRequest.source(searchSourceBuilder);

        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = search.getScrollId();
        SearchHit[] hits = search.getHits().getHits();

        while (hits != null && hits.length >0 ){
            System.out.println(scrollId);
            System.out.println(hits);
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll(scroll);

            SearchResponse scroll1 = restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
            scrollId = scroll1.getScrollId();
            hits = scroll1.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);

        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        boolean succeeded = clearScrollResponse.isSucceeded();
        System.out.println(succeeded);

    }

}
