package com.sky.es.service;

import com.sky.es.domain.ElasticEntity;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;

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
//        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();

        log.info("index:{},id{},version:{}",index,id,version);
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
        String[] includes = new String[]{"message","*Date","user","name"};
        // 排除的字段 排除与包含字段有相同的字段的时候，会排除掉
//        String[] excludes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
//        request.version(2);
        ElasticEntity elasticEntity = new ElasticEntity();
//        request.storedFields("message");
        try {
            GetResponse getResponse = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            System.out.println("version is " + getResponse.getVersion());
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
//            String message = getResponse.getField("message").getValue();
            elasticEntity.setId(getResponse.getId());
//            elasticEntity.setData(message);
            elasticEntity.setIndex(index);
            elasticEntity.setData(sourceAsMap);
            log.info("get source is : {}",sourceAsMap);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return elasticEntity;
    }
    
    public boolean existsData(String index,String id) {
        GetRequest request = new GetRequest(index, id);
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        try {
            return restHighLevelClient.exists(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void existsAsyncData(String index,String id){
        GetRequest request = new GetRequest(index, id);
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                if(aBoolean){
                    System.out.println("exist........................................");
                }else{
                    System.out.println("asd..............................................");
                }
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
                System.out.println("asadsdsa................................" + e.getCause());
            }
        };
        restHighLevelClient.existsAsync(request,RequestOptions.DEFAULT,listener);
    }

    public void deleteData(String index,String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(index, id);
        // .setIfSeqNo(100).setIfPrimaryTerm(2)
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        if(delete.getResult() == DocWriteResponse.Result.NOT_FOUND){
            System.out.println("未查到=======================================");
            return;
        }

        String id1 = delete.getId();

        String index1 = delete.getIndex();

        log.info("id：{}，index：{}，seqNO:{},PrimaryTerm：{}",id1,index1,delete.getSeqNo(),delete.getPrimaryTerm());

        ReplicationResponse.ShardInfo shardInfo = delete.getShardInfo();
        // 成功分片数不等于总分片数
        if(shardInfo.getTotal() != shardInfo.getSuccessful()){
            System.out.println("成功分片数不等于总分片数");
        }
        // 失败的分片
        if(shardInfo.getFailed() > 0){
            Stream.of(shardInfo.getFailures()).forEach(failure -> System.out.println(failure.reason()));
        }
    }

    public void updateData(String index,String id) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, id);
//        Map<String, Object> param = singletonMap("count", 4);
//        Script painless = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", param);
//        updateRequest.script(painless);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.timeField("update",new Date());
            xContentBuilder.field("reson","updatess");
        }
        xContentBuilder.endObject();
        updateRequest.doc(xContentBuilder);
//        updateRequest.detectNoop(true);
        // 若文档不存在，会创建
        updateRequest.scriptedUpsert(true);
        // 文档不存在，必须作upsert更新插入操作
        updateRequest.docAsUpsert(true);
        // 失败重试次数
        updateRequest.retryOnConflict(3);

        // 更新之前必粗处于活跃的分片副本数
//        updateRequest.waitForActiveShards(2);
//        updateRequest.waitForActiveShards(ActiveShardCount.ALL);
        try {
            restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
