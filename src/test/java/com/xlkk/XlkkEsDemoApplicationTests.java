package com.xlkk;

import com.alibaba.fastjson.JSON;
import com.xlkk.pojo.User;
import lombok.SneakyThrows;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class XlkkEsDemoApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;
    @Test
    void contextLoads() throws IOException {
        //??????????????????
//        createIndex();
        //??????????????????
//        getIndex();
        //??????????????????
//        deleteIndex("xlkk_index");
        //??????????????????
//        testAddDoc();
        //????????????????????????
//        testDocExists();
        //????????????
//        testGetDoc();
        //????????????
//        testUpdateDoc();
        //????????????
//        testDeleteDoc();
        //????????????????????????
//        testBulkRequst();
        //????????????
        testSearch();
    }


    //?????????????????????
    void createIndex() throws IOException {
        //????????????
        CreateIndexRequest request = new CreateIndexRequest("xlkk_index");
        //?????????????????????,?????????????????????
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.index());
    }
    //????????????
    void getIndex() throws IOException {
        GetIndexRequest index = new GetIndexRequest("xlkk_index");
        boolean exists = client.indices().exists(index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //??????????????????
    void deleteIndex(String index) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    //test add document
    void testAddDoc() throws IOException {
        User user = new User("xlkk", 21);
        //????????????
        IndexRequest request = new IndexRequest("xlkk_index");

        //???????????????PUT xlkk_index/_doc/1
        request.id("1");

        //??????????????????????????????
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //?????????????????????
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
    }
    //?????????????????????????????????
    void testDocExists() throws IOException {
        GetRequest getRequest = new GetRequest("xlkk_index", "1");
        //?????????????????????????????????????????????????????????????????????????????????????????????
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //??????????????????
    void testGetDoc() throws IOException {
        GetRequest getRequest = new GetRequest("xlkk_index","1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());

    }
    //??????????????????
    void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("xlkk_index", "1");
        User lk = new User("lk", 23);
        updateRequest.doc(JSON.toJSONString(lk),XContentType.JSON);
        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }
    //??????????????????
    void testDeleteDoc() throws IOException{
        DeleteRequest deleteRequest = new DeleteRequest("xlkk_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }
    //????????????????????????
    void testBulkRequst() throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("xlkk1",21));
        userList.add(new User("xlkk2",22));
        userList.add(new User("xlkk3",23));
        userList.add(new User("xlkk4",24));
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(new IndexRequest("xlkk_index")
                            .id(""+(i+1))
                            .source(JSON.toJSONString(userList.get(i)),XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());

    }
    //??????
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("xlkk_index");
        //??????????????????
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //????????????,??????????????????QueryBuilders?????????
        //QureyBuilders.termQuery??????????????????
        //QureyBuilders.matchAllQuery????????????????????????

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "xlkk1");
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("=========================================");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }

    }
}
