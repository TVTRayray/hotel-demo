package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.List;


@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient client;



    /*-----------------------------------------------文档CRUD---------------------------------------*/

    //添加文档数据
    @Test
    void testAddDocument() throws IOException {
        //根据ID查询酒店数据
        Hotel hotel = hotelService.getById(61075L);
        //转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);

        //1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        //2.准备JSON文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        //3.发送请求
        client.index(request,RequestOptions.DEFAULT);
    }

    //获取文档数据
    @Test
    void testGetDocumentById() throws IOException {
        //1.准备request
        GetRequest request = new GetRequest("hotel", "61075");
        //2.发送请求，得到响应
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        //3.解析响应结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

        System.out.println(hotelDoc);
    }
    //更新文档（局部）
    @Test
    void  testUpdateDocumentById() throws IOException {
        //1.准备request
        UpdateRequest request = new UpdateRequest("hotel", "61075");
        //2.准备参数，每两个参数为一对（key,value）
        request.doc(
                "price","1153"
//                ,"name","Rose"
        );
        //3.更新文档
        client.update(request,RequestOptions.DEFAULT);
    }

    //删除文档
    @Test
    void  testDeleteDocumentById() throws IOException {
        //1.准备request
        DeleteRequest request = new DeleteRequest("hotel", "61075");
        //2.删除文档
        client.delete(request, RequestOptions.DEFAULT);
    }

    //批量导入文档到索引库中
    @Test
    void testBulkRequest() throws IOException {
        List<Hotel> hotels = hotelService.list();
        //1.准备request
        BulkRequest request = new BulkRequest();
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //2.准备参数，添加多个新增Request
            request.add(new IndexRequest("hotel").
                    id(hotelDoc.getId().toString()).
                    source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        //3.发送请求
        client.bulk(request,RequestOptions.DEFAULT);
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
