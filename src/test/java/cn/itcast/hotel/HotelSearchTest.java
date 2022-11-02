package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

@SpringBootTest

public class HotelSearchTest {

    @Autowired
    private IHotelService ihotelService;
    private RestHighLevelClient client;

    @Test
    void testMatchAll() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.结果处理
        /** 拿到的json文件格式如下，需要获取hits里面的内容，再逐步通过get获取数据
         *  {
         *   "took" : 96,
         *   "timed_out" : false,
         *   "hits" : {
         *     "total" : {
         *       "value" : 30,
         *       "relation" : "eq"
         *     },
         *     "max_score" : 2.7571862,
         *     "hits" : [
         *       {
         *         "_index" : "hotel",
         *         "_type" : "_doc",
         */
        handleResponse(response);
    }

    @Test
    void testMatch() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testBool() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2添加term
        boolQuery.must(QueryBuilders.termQuery("city","上海"));
        //2.2添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").gte(200).lte(300));

        request.source().query(boolQuery);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }
    @Test
    void testPageAndSort() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1query
        request.source().query(QueryBuilders.matchAllQuery());
        //2.2排序 sort
        request.source().sort("price", SortOrder.ASC);
        //2.3分页 from、size
        request.source().from(5).size(10);
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }
    @Test
    void testHighlight() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        //2.1query
        request.source().query(QueryBuilders.matchQuery("name","如家"));
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    //抽取结果处理以及反序列、高亮等操作
    private void handleResponse(SearchResponse response) {
        //4.结果处理
        SearchHits searchHits = response.getHits();
        //4.1查询总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("total:"+total);
        //4.2查询的结果转为数组
        SearchHit[] hits = searchHits.getHits();
        //4.3遍历
        for (SearchHit hit : hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            /** 高亮json数据格式
             * "highlight" : {
             *           "name" : [
             *             "<em>如家</em>酒店(北京良乡西路店)"
             *           ]
             *         }
             */
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if ( !CollectionUtils.isEmpty(highlightFields) ){
                //根据字段名称获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if(highlightField != null){
                    //获取高亮值
                    String name = highlightField.getFragments()[0].string();
                    //覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
            System.out.println(hotelDoc);
        }
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
