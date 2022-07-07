package com.xlkk;

import com.xlkk.pojo.User;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

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
        deleteIndex("xlkk_index");
    }
    //测试索引的创建
    @Test
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

    void testAddDoc(){
        User user = new User("xlkk", 21);
        //创建请求

    }

}
