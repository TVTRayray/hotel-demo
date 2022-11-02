package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Date;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;
@SpringBootTest
public class HotelIndexTest {

    @Autowired
    private RestHighLevelClient client;

    @Test
    void  testInit(){
        System.out.println(client);
    }

    /*-----------------------------------------------索引库-------------------------------------*/
    //创建索引库
    @Test
    void createHotelIndex() throws IOException {
        // 1.创建request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2.准备请求的参数：DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);

    }

    //删除索引库
    @Test
    void deleteHotelIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");

        client.indices().delete(request,RequestOptions.DEFAULT);
    }


    //判断索引库是否存在
    @Test
    void testExistsHotelIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);

        System.err.println(exists?"索引库存在":"索引库不存在");

    }


    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://10.16.108.122:9200")
        ));
    }
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
