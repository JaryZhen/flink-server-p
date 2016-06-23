package com.flink; /**
 * Created by jaryzhen on 6/12/16.
 */


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.util.Es_Utils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by jaryzhen on 5/23/16.
 */
public class estest {


    static Es_Utils es_utils = new Es_Utils();

    public static void main(String [] a) throws UnknownHostException {
        getData();
        ///getESData(3000);
    }

    //获取此index的所有数据
    public static List getESData(int size) throws UnknownHostException {

        ArrayList datalist = null;
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "logis_es2").build();

        Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.82"), 9300));
        // .addTransportAddress(new InetSocketTransportAddress("192.168.1.82", 9300));

        System.out.println("scroll 模式启动！");
        Date begin = new Date();
        SearchResponse scrollResponse = client.prepareSearch("loginsight_20")
                .setSearchType(SearchType.SCAN).setSize(10000).setScroll(TimeValue.timeValueMinutes(1))
                .execute().actionGet();
        long count = scrollResponse.getHits().getTotalHits();//第一次不返回数据
        datalist = new ArrayList<>();
        for (int i = 0, sum = 0; sum < size; i++) {
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            sum += scrollResponse.getHits().hits().length;
            System.out.println(scrollResponse.toString());
            System.out.println(scrollResponse.getFailedShards());
            datalist.addAll(toJSON(scrollResponse.toString()));
            System.out.println("总量" + count + " 已经查到" + sum);

        }

        System.out.println("耗时: " + dataComt(new Date().getTime(), begin.getTime()));
        client.close();
        return datalist;
    }

    public static List toJSON(String s) throws UnknownHostException {

        //System.out.print(str);
        JSONObject jsonObject = JSON.parseObject(s);
        JSONObject jsonObject1 = jsonObject.getJSONObject("hits");
        JSONArray array = jsonObject1.getJSONArray("hits");
        Object[] a = array.toArray();

        ArrayList datalist = null;
        int total = jsonObject1.getInteger("total");
        System.out.println(total);
        System.out.println(a.length);

        if (total > 0) {
            datalist = new ArrayList();
            for (int i = 0; i < a.length; i++) {
                System.out.println("");
//                System.out.println(a[i].toString());
                JSONObject o = JSON.parseObject(a[i].toString());
                JSONObject source = o.getJSONObject("_source");
                String mesg = source.getString("message");
                datalist.add(mesg);
            }
        }
        return datalist;
    }
    public void test() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "logis_es2").build();

        Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.82"), 9300));
        // .addTransportAddress(new InetSocketTransportAddress("192.168.1.82", 9300));

//Add transport addresses and do something with the client...

        SearchResponse response = client.prepareSearch("loginsight_0")
                .setTypes("message")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                //.setQuery(QueryBuilders.termQuery("message.full_message", "*"))                 // Query
                //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(10).setExplain(true)
                .execute()
                .actionGet();
        // SearchResponse response = client.prepareSearch().execute().actionGet();
        response.getHits();
        System.out.println(response.toString());
        client.close();
    }

    public static QueryBuilder scorll(String from, String to) throws UnknownHostException {
        //"2016-05-17 22:51:49.413"   ----   "2016-05-17 23:33:41.914"
        //QueryBuilders.rangeQuery("timestamp").from("2016-05-17 22:51:49.413").to("2016-05-18 00:33:41.987");
        return QueryBuilders.rangeQuery("timestamp").from(from).to(to);
    }

    //获取 时间段 数据
    public static List getESDataByTimestamp(QueryBuilder queryBuilder) throws UnknownHostException {

        ArrayList datalist = null;
        //预准备执行搜索
        Client client =null;// es_utils.startupClient();


        System.out.println("scroll 模式启动！");
        Date begin = new Date();

        SearchResponse scrollResponse = client.prepareSearch("").setQuery(queryBuilder)
                .setSearchType(SearchType.SCAN).setSize(10000).setScroll(TimeValue.timeValueMinutes(1))
                .execute().actionGet();
        // System.out.println(scrollResponse.toString());
        long count = scrollResponse.getHits().getTotalHits();//第一次不返回数据
        System.out.println("total: " + count);
        datalist = new ArrayList<>();
        for (int i = 0, sum = 0; sum < count; i++) {
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            sum += scrollResponse.getHits().hits().length;
             System.out.println(scrollResponse.toString());
            datalist.addAll(toJSON(scrollResponse.toString()));
            System.out.println("总量" + count + " 已经查到" + sum);
        }
        System.out.println("耗时: " + dataComt(new Date().getTime(), begin.getTime()));
        client.close();
        return datalist;
    }

    //restfull的形式获取
    public static List getData() {

        String cm2 = "curl -XGET 'http://192.168.70.180:9200/_search?pretty' -d '{\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 10000,\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": {\n" +
                "        \"match_all\": {}\n" +
                "      },\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": {\n" +
                "            \"range\": {\n" +
                "              \"timestamp\": {\n" +
                "                \"from\": \"2016-05-17 16:52:35.000\",\n" +
                "                \"to\": \"2016-05-19 04:55:39.000\",\n" +
                "                \"include_lower\": true,\n" +
                "                \"include_upper\": true\n" +
                "              }\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"sort\": [\n" +
                "    {\n" +
                "      \"timestamp\": {\n" +
                "        \"order\": \"desc\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}'";


        String str = exec(cm2).toString();
        //System.out.print(str);
        JSONObject jsonObject = JSON.parseObject(str);
        JSONObject jsonObject1 = jsonObject.getJSONObject("hits");
        JSONArray array = jsonObject1.getJSONArray("hits");
        Object[] a = array.toArray();

        ArrayList datalist = null;
        int total = jsonObject1.getInteger("total");
        System.out.println(total);
        System.out.println(a.length);

        if (total > 0) {
            datalist = new ArrayList();
            for (int j = 0; j < a.length; j++) {
                JSONObject o = JSON.parseObject(a[j].toString());
                System.out.print("");
                JSONObject source = o.getJSONObject("_source");
                String mesg = source.getString("message");
                datalist.add(mesg);
            }
        }
        return datalist;
    }

    public static Object exec(String cmd) {
        try {
            String[] cmdA = {"/bin/sh", "-c", cmd};
            Process process = Runtime.getRuntime().exec(cmdA);
            LineNumberReader br = new LineNumberReader(new InputStreamReader(
                    process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // final long ti  =new Date().getTime();
    public static String dataComt(long current, long last) {
        long c = (current - last) / 1000;
        return c + "s ";
    }
}