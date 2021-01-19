package com.ygq.run.es;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsTest {

    private Logger log = LoggerFactory.getLogger("EsTest");

    @Test
    public void createIndex() {
        createIndex("school");
    }

    @Test
    public void createTemplate() throws IOException {
        createTemplate("school");
    }

    @Test
    public void batchInsert() {
        Student student = new Student();
        student.setName("孙尚香");
        student.setSex("女");
        student.setAge(40);
        student.setAddress("湖北省武汉市洪山区保利国际中心222");
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("125", JSON.toJSONString(student));
        batchReplace("school-202101", map);
    }

    public void createTemplate(String index) throws IOException {
        List<String> indexNames = Collections.singletonList(index + "*");
        String setting = "{\n" +
                "  \"index\": {\n" +
                "    \"number_of_shards\": \"3\",\n" +
                "    \"blocks\": {\n" +
                "      \"read_only_allow_delete\": \"false\"\n" +
                "    },\n" +
                "    \"number_of_replicas\": \"1\",\n" +
                "    \"max_result_window\": \"2147483647\"\n" +
                "  }\n" +
                "}";
        String mapping = "{\n" +
                "      \"properties\": {\n" +
                "        \"address\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"age\": {\n" +
                "          \"type\": \"long\"\n" +
                "        },\n" +
                "        \"name\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"sex\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        //设置别名
        String aliasName = String.format("%s-alias", index);
        String templateName = String.format("%stemplate", index);
        createTemplate(templateName, indexNames, setting, mapping, aliasName);
        log.info("创建{} 模板...", templateName);
    }

    public AcknowledgedResponse createTemplate(String templateName, List<String> indexNames, String setting, String mapping, String aliasName) throws IOException {
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName);
        request.settings(setting, XContentType.JSON);
        request.mapping(mapping, XContentType.JSON);
        request.order(0);
        request.create(true);
        request.patterns(indexNames);
        if (aliasName != null && aliasName.length() > 0) {
            request.alias(new Alias(aliasName));
        }
        return getClient().indices().putTemplate(request, RequestOptions.DEFAULT);
    }

    public boolean batchReplace(String index, Map<String, String> valueMap) {
        try {
            BulkResponse bulkRequest = createDoc(index, valueMap);
            if (null != bulkRequest) {
                int other_count = 0;
                int create_count = 0;
                for (BulkItemResponse bulkItemResponse : bulkRequest.getItems()) {
                    if (bulkItemResponse.status() == RestStatus.CREATED) {
                        create_count++;
                    } else {
                        log.debug("es写入数据状态 {}", bulkItemResponse.status().toString());
                        other_count++;
                    }
                }
                log.info("{} create {} 其他 {}", index, create_count, other_count);
                return true;
            }
        } catch (Exception e) {
            log.error("数据写入Es异常", e);
        }
        return false;
    }

    public BulkResponse createDoc(String indexName, Map<String, String> valueMap) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (String id : valueMap.keySet()) {
            IndexRequest request = new IndexRequest(indexName);
            request.index(indexName).id(id).source(valueMap.get(id), XContentType.JSON);
            request.onRetry();
            request.opType(DocWriteRequest.OpType.CREATE);
            request.waitForActiveShards(ActiveShardCount.ALL);
            bulkRequest.add(request);
        }
        return getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    public boolean createIndex(String index) {
        CreateIndexRequest request = new CreateIndexRequest(index);//创建索引
        //创建的每个索引都可以有与之关联的特定设置。
        String setting = "{\n" +
                "  \"index\": {\n" +
                "    \"number_of_shards\": \"3\",\n" +
                "    \"blocks\": {\n" +
                "      \"read_only_allow_delete\": \"false\"\n" +
                "    },\n" +
                "    \"number_of_replicas\": \"1\",\n" +
                "    \"max_result_window\": \"2147483647\"\n" +
                "  }\n" +
                "}";
        request.settings(setting, XContentType.JSON);
        String mapping = "{\n" +
                "      \"properties\": {\n" +
                "        \"address\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"age\": {\n" +
                "          \"type\": \"long\"\n" +
                "        },\n" +
                "        \"name\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        \"sex\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"fields\": {\n" +
                "            \"keyword\": {\n" +
                "              \"type\": \"keyword\",\n" +
                "              \"ignore_above\": 256\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }";
        //创建索引时创建文档类型映射
        request.mapping(mapping, XContentType.JSON);

        //为索引设置一个别名
        request.alias(
                new Alias(String.format("%s-alias", index))
        );
        //可选参数
        request.setTimeout(TimeValue.timeValueMinutes(2));//超时,等待所有节点被确认(使用TimeValue方式)
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));//连接master节点的超时时间(使用TimeValue方式)
        //request.waitForActiveShards(ActiveShardCount.DEFAULT);
        request.waitForActiveShards(ActiveShardCount.DEFAULT);//在创建索引API返回响应之前等待的活动分片副本的数量，以ActiveShardCount形式表示。
        try {
            //同步执行
            getClient().indices().create(request, RequestOptions.DEFAULT);
            log.info("{} create success !", index);
            return true;
        } catch (IOException e) {
            log.error("创建index异常{}：", index, e);
        }
        return false;
    }

    private RestHighLevelClient getClient() {
        HttpHost[] httpHosts = new HttpHost[3];
        httpHosts[0] = new HttpHost("localhost", 9200, "http");
        httpHosts[1] = new HttpHost("localhost", 9201, "http");
        httpHosts[2] = new HttpHost("localhost", 9202, "http");

        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                requestConfigBuilder.setConnectTimeout(1000);
                requestConfigBuilder.setSocketTimeout(3000);
                requestConfigBuilder.setConnectionRequestTimeout(1000);
                return requestConfigBuilder;
            }
        });
        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.setMaxConnTotal(50);
                httpClientBuilder.setMaxConnPerRoute(10);
                return httpClientBuilder;
            }
        });
        return new RestHighLevelClient(restClientBuilder);
    }
}
