package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            //1.准备Request
            SearchRequest request = new SearchRequest("hotel");
            //2.准备DSL
            //2.1query
            buildBasicQuery(params, request);
            //2.2分页
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page-1)*size).size(size);

            //2.3排序
            String location = params.getLocation();
            if(location != null && !location.equals("")){
                request.source().sort(SortBuilders
                        .geoDistanceSort("location",new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }
            //3.发送请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            //4.处理响应
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        //1.构建booleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        String key = params.getKey();
        //关键字搜索
        if(key == null || "".equals(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        }
        else {
           boolQuery.must(QueryBuilders.matchQuery("all",key));
        }
        //条件过滤
        //城市过滤
        if(params.getCity() != null && !params.getCity().equals("")){
            boolQuery.must(QueryBuilders.termQuery("city", params.getCity()));
        }
        //品牌过滤
        if(params.getBrand() != null && !params.getBrand().equals("")){
            boolQuery.must(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        //星级过滤
        if(params.getStarName() != null && !params.getStarName().equals("")){
            boolQuery.must(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        //价格过滤
        if(params.getMaxPrice() != null && params.getMinPrice() != null){
            boolQuery.filter(QueryBuilders.
                    rangeQuery("price").
                    gte(params.getMinPrice()).
                    lte(params.getMaxPrice())
            );
        }

        //2.算分控制
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        //原始的条件查询
                        boolQuery,
                        //function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中的一个function score元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        //算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(functionScoreQuery);
    }

    private PageResult handleResponse(SearchResponse response) {
        //4.结果处理
        SearchHits searchHits = response.getHits();
        //4.1查询总条数
        long total = searchHits.getTotalHits().value;
        //4.2查询的结果转为数组
        SearchHit[] hits = searchHits.getHits();
        //4.3遍历
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            //获取文档source
            String json = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            //获取排序值，实际上是采用地理坐标排序后，sortValues分数就是距离值
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotels.add(hotelDoc);
            }
        return new PageResult(total,hotels);
    }
}
