package com.nmsl;

import com.alibaba.fastjson.JSON;
import com.nmsl.domain.User;
import com.sun.org.apache.bcel.internal.generic.NEW;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class NmslEsApiApplicationTests {

    @Resource
    private RestHighLevelClient restHighLevelClient;

    //测试创建索引
    @Test
    void contextLoads() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("nmsl_index");
        //2.客户端执行请求 indicesclient ,请求后获得响应
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }


    //测试获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("wuhu_index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    //测试删除索引
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(".apm-agent-configuration");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("芜湖", 3);
        //创建请求
        IndexRequest request = new IndexRequest("nmsl_index");

        //规则 put/nmsl_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");

        //将数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求,获取响应结果
        IndexResponse index = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        System.out.println(index.toString());
        System.out.println(index.status());
    }

    //获取文档/判断是否存在
    @Test
    void testIsExists() throws IOException {
        GetRequest request = new GetRequest("nmsl_index", "1");
        //不获取返回的 _sources 的上下文了
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none");

        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //获取文档信息
    @Test
    void testGet() throws IOException {
        GetRequest request = new GetRequest("nmsl_index", "1");
        //不获取返回的 _sources 的上下文了
        GetResponse documentFields = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(documentFields.getSourceAsString());
    }

    //更新文档信息
    @Test
    void testUpdateDoucement() throws IOException {
        UpdateRequest request = new UpdateRequest("nmsl_index", "1");
        //不获取返回的 _sources 的上下文了
        request.timeout("15s");
        User user = new User("妈耶",18);
        request.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse update = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }

    //删除文档
    @Test
    void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest("nmsl_index", "1");
        request.timeout("1s");
        //不获取返回的 _sources 的上下文了
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    //批量插入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest request = new BulkRequest();
        request.timeout("10s");

        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("陈鑫",18));
        userList.add(new User("王志涛",22));
        userList.add(new User("罗汉伟",32));
        userList.add(new User("芜湖",2));

        for (int i = 0; i < userList.size(); i++) {
            request.add(new IndexRequest("nmsl_index")
            .id(""+(i+1))
            .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }

        BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());
    }

    //查询
    //SearchSourceBuilder 条件构造
    //searchSourceBuilder.highlighter(); 构建高亮
    //TermQueryBuilder  精确查询
    //matchAllQuery 完全匹配
    @Test
    void testSearch() throws IOException {
        SearchRequest request = new SearchRequest("nmsl_index");

        //构建搜索的条件   建造者模式
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询条件,用QueryBuilders.termQuery精确匹配     matchAllQuery 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "陈鑫");
        searchSourceBuilder.query(termQueryBuilder);
        /* 分页,不写也有默认参数
        searchSourceBuilder.from();
        searchSourceBuilder.size();
        */
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        SearchRequest source = request.source(searchSourceBuilder);
        SearchResponse search = restHighLevelClient.search(request, RequestOptions.DEFAULT);

        System.out.println(JSON.toJSONString(search.getHits()));

        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
