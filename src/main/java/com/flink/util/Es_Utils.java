package com.flink.util;
/**
 * Created by jaryzhen on 6/7/16.
 */

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * startup and shutDownClient ----》Client
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
public class Es_Utils {

    protected static final String INDEX_01;
    protected static final String INDEX_TYPE;
    protected static final String CLUSTER_NAME;
    protected static final String IP;
    protected static final Integer PORT;

    protected static final Integer SCROLLSIZE;


    protected static Client client;

//    public static void main (String[] a) {
//
//        new Es_Utils();
//    }

    //    cluster=logis_es2
//    index_0=loginsight_20
//    type_0=message
//    ip=192.168.1.82
//    port=9300
    static {
        InputStream inputStream = Es_Utils.class.getClassLoader().getResourceAsStream("es.properties");
        Properties p = new Properties();
        try {
            p.load(inputStream);
        } catch (IOException e1) {
            e1.printStackTrace();

        }
        INDEX_01 = p.getProperty("index_0");
        INDEX_TYPE = p.getProperty("type_0");
        CLUSTER_NAME = p.getProperty("cluster");
        IP = p.getProperty("ip");
        PORT = Integer.parseInt(p.getProperty("port"));
        SCROLLSIZE = Integer.parseInt(p.getProperty("scroll_size"));
        System.out.println("cluster:" + p.getProperty("cluster") + ",index:" + p.getProperty("index_0")+",ip:" + p.getProperty("ip"));

    }

    /**
     * startup Transport Client
     * 启动es
     */
    protected static Client startupClient() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", CLUSTER_NAME).build();

         client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(IP), PORT));
        return client;
    }

    /**
     * on shutDownClient
     * 停止es
     */
    protected static void shutDownClient() {
        client.close();
    }


    /**
     * 获取所有index
     */
    protected static void getAllIndices() {
        ActionFuture<IndicesStatsResponse> isr = client.admin().indices().stats(new IndicesStatsRequest().all());
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        Map<String, IndexStats> indexStatsMap = isr.actionGet().getIndices();
        Set<String> set = isr.actionGet().getIndices().keySet();
        //set.forEach(System.out::println);
    }

    /**
     * 打印SearchResponse结果集
     *
     * @param response response
     */
    protected static void printSearchRespons(SearchResponse response) {
        SearchHit[] searchHitsByPrepareSearch = response.getHits().hits();
        //获取结果集打印
        for (SearchHit searchHit : searchHitsByPrepareSearch) {
            System.out.println(searchHit.getSourceAsString());
        }
    }


}