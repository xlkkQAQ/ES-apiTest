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
        //测试创建索引
//        createIndex();
        //测试获取索引
//        getIndex();
        //测试删除索引
//        deleteIndex("xlkk_index");
        //测试创建文档
//        testAddDoc();
        //判断文档是否存在
//        testDocExists();
        //获取文档
//        testGetDoc();
        //更新文档
//        testUpdateDoc();
        //删除文档
//        testDeleteDoc();
        //测试批量插入数据
//        testBulkRequst();
        //测试查询
        testSearch();
    }


    //测试索引的创建
    void createIndex() throws IOException {
        //创建请求
        CreateIndexRequest request = new CreateIndexRequest("xlkk_index");
        //客户端执行请求,请求后获得响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response.index());
    }
    //获取索引
    void getIndex() throws IOException {
        GetIndexRequest index = new GetIndexRequest("xlkk_index");
        boolean exists = client.indices().exists(index, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //测试删除索引
    void deleteIndex(String index) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    //test add document
    void testAddDoc() throws IOException {
        User user = new User("xlkk", 21);
        //创建请求
        IndexRequest request = new IndexRequest("xlkk_index");

        //请求规则：PUT xlkk_index/_doc/1
        request.id("1");

        //将我们的数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);

        //客户端发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        System.out.println(response.toString());
    }
    //获取文档，判断是否存在
    void testDocExists() throws IOException {
        GetRequest getRequest = new GetRequest("xlkk_index", "1");
        //下面操作可以过滤上下文，这样我们就可以不用获取上下文，效率更高
        getRequest.fetchSourceContext(new FetchSourceContext(false));

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //获取文档信息
    void testGetDoc() throws IOException {
        GetRequest getRequest = new GetRequest("xlkk_index","1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());

    }
    //更新文档信息
    void testUpdateDoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("xlkk_index", "1");
        User lk = new User("lk", 23);
        updateRequest.doc(JSON.toJSONString(lk),XContentType.JSON);
        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }
    //删除文档记录
    void testDeleteDoc() throws IOException{
        DeleteRequest deleteRequest = new DeleteRequest("xlkk_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }
    //测试批量添加数据
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
    //查询
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("xlkk_index");
        //构建查询条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件,我们可以使用QueryBuilders来实现
        //QureyBuilders.termQuery表示精确查询
        //QureyBuilders.matchAllQuery表示匹配所有查询

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
