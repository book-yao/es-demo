package com.sky.es.service;

import com.sky.es.domain.ElasticEntity;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 索引请求service
 *
 * @author JWF
 * @date 2019 /10/25
 */
@Service
@Slf4j
public class IndexRequestService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * Create index *
     *
     * @throws IOException io exception
     */
    public void createIndex() throws IOException {
        IndexRequest request = new IndexRequest("posts");
        request.id("4");
        Map<String,Object> jsonMap = new HashMap<String,Object>(){{
            put("user","jwf");
            put("postDate",new Date());
            put("message","try out es");
            put("trace","this is trace");
            put("tags",new String[]{"aa","bb"});
        }};
        request.source(jsonMap);

        request.routing("routing111");

        request.timeout(TimeValue.timeValueSeconds(1));
//        request.timeout("1s");

        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
//        request.setRefreshPolicy("wait_for");

        // 版本号
//        request.version(2);
//        request.versionType(VersionType.EXTERNAL);

        request.opType(DocWriteRequest.OpType.CREATE);
//        request.opType("create");

//        request.setPipeline("pipeline");

        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();

        log.info("index:{},type:{},id{},version:{}",index,type,id,version);
    }

    /**
     * 获取单条数据
     * @param index
     * @param id
     * @return
     */
    public ElasticEntity getIndexData(String index,String id){
        GetRequest request = new GetRequest(index, id);
        request.routing("routing");

        // 包含的字段
        String[] includes = new String[]{"message","*Date","user"};
        // 排除的字段 排除与包含字段有相同的字段的时候，会排除掉
//        String[] excludes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"message"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
        ElasticEntity elasticEntity = new ElasticEntity();
        try {
            GetResponse getResponse = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
//            String message = getResponse.getField("message").getValue();
            elasticEntity.setId(getResponse.getId());
            elasticEntity.setIndex(index);
            elasticEntity.setData(sourceAsMap);
            log.info("get source is : {}",sourceAsMap);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return elasticEntity;
    }
}
