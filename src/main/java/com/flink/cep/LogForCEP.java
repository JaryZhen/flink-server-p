/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.cep;


import com.flink.entry.LogEntity;
import com.flink.entry.Parma;
import com.flink.util.Es_SearchImpl;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.log4j.Logger;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flink.util.Es_SearchImpl.getDataByScorll;
import static com.flink.util.Es_SearchImpl.getESData;


@SuppressWarnings("serial")
public class LogForCEP {

    public static final Logger log = Logger.getLogger(LogForCEP.class);
    static List<LogEntity> logEntityList = null;
    static List<LogEntity> u_and_c = null;
    static List<LogEntity> only_u = null;
    static List<LogEntity> only_c = null;
    static List<LogEntity> none = null;

//    public static void main(String agrs[]) throws Exception {
//
//        QueryBuilder rangeQuery =QueryBuilders.rangeQuery("timestamp").from("2016-06-19 07:51:49.413").to("2016-06-21 07:33:41.987");
//        QueryBuilder scorllQuery_bool = QueryBuilders.boolQuery()
//                // .must(QueryBuilders.matchQuery("message", "TID"))
//                .must(QueryBuilders.matchQuery("gl2_source_input", "5767b8aee9335e0d0fa02b37"))
//                .must(rangeQuery);
//        List es_all_list = getDataByScorll(scorllQuery_bool);
//        System.out.println(es_all_list.size());
//    }

    public static List<LogEntity> getLog(Parma parma) throws Exception {
        cep(parma);
        return logEntityList;
    }

    public static void cep(final Parma parma) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.setParallelism(2);


        String search = parma.getSearch();

        Long logsize = parma.getLogsize();

        String uid = parma.getUid();

        String[] transaction = parma.getTransaction();
        String group=null;
        String start = transaction[1];
        String follow = transaction[2];
        String end = transaction[3];

        Integer within = parma.getWithin();
        String contain = parma.getContains();

        DataStream<String> datas = null;
        List es_all_list = null;
        log.info("search=" + search);
        if (!search.contains("*")) {
            log.info("search not contains ****");
            //gl2_remote_ip:200 and source:example.org and timestamp= XXX_XXX
            //  List<QueryBuilder> queryBuilderList = parselSPL(search);

            String[] arry_spl = search.split("_and_");
            String timestamp = search.substring(search.indexOf("timestamp") + 10, search.length());
            System.out.println("timestamp" + timestamp);
            String from_t = timestamp.split("_")[0], to_t = timestamp.split("_")[1];
            String from = from_t.substring(0, from_t.length() - 1).replace('T', ' ');
            String to = to_t.substring(0, to_t.length() - 1).replace('T', ' ');

            QueryBuilder query_timestamp = QueryBuilders.rangeQuery(Es_SearchImpl.TIMESTAMP).from(from).to(to);
            QueryBuilder match = QueryBuilders.matchQuery(from, to);
            List<String> querylist = new ArrayList<>();

            for (int i = 0; i < arry_spl.length; i++) {
                if (!arry_spl[i].contains("timestamp")) {
                    String a = arry_spl[i].split(":")[0].trim();
                    String b = arry_spl[i].split(":")[1].trim();
                    querylist.add(a);
                    querylist.add(b);
                }
            }
            log.info("queryBuilderList.size()= " + querylist.size());
            if (arry_spl.length == 2) {
                log.info("operator=2 \n");
                QueryBuilder scorllQuery_bool = QueryBuilders.boolQuery().must(query_timestamp).must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1)));

                es_all_list = getDataByScorll(scorllQuery_bool,logsize);
            } else if (arry_spl.length == 3) {
                log.info("operator=3 ");
                QueryBuilder scorllQuery_bool = QueryBuilders.boolQuery().must(query_timestamp).must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1)))
                        .must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1)));

                es_all_list = getDataByScorll(scorllQuery_bool,logsize);
            } else if (arry_spl.length == 4) {
                log.info("operator=4");
                QueryBuilder scorllQuery_bool = QueryBuilders.boolQuery().must(query_timestamp).must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1)))
                        .must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1))).must(QueryBuilders.matchQuery(querylist.get(0), querylist.get(1)));
                es_all_list = getDataByScorll(scorllQuery_bool,logsize);
            } else {
                LogEntity b = new LogEntity();
                b.setIndex("500");
                b.setMessage("sorry no more than 4 operators");
                logEntityList = new ArrayList<>();
                logEntityList.add(b);
                return;
            }
            if (es_all_list.size() == 0) {
                LogEntity b = new LogEntity();
                b.setIndex("500");
                b.setMessage("sorry no data");
                logEntityList = new ArrayList<>();
                logEntityList.add(b);
                return;
            }
            datas = env.fromCollection(es_all_list);

        } else {
            log.info("search contains ****");
            es_all_list = getESData(logsize);
            log.info("getESData size ==" + es_all_list.size());
            if (es_all_list.size() == 0) {
                LogEntity b = new LogEntity();
                b.setIndex("500");
                b.setMessage("sorry no data ... ");
                logEntityList = new ArrayList<>();
                logEntityList.add(b);
                return;
            }
            datas = env.fromCollection(es_all_list);
        }


        DataStream<Event_d> input = dataETL(datas);

        Pattern<Event_d, ?> pattern = definePattern(start, follow, end, within);
        //logEntityList = new ArrayList<>();

        getCEPResult(input, pattern, uid, contain, start, follow);

        //
        env.execute();
    }

    public static List<QueryBuilder> parselSPL(String spl) {

        String[] arry_spl = spl.split("_and_");
        List<QueryBuilder> queryBuilderList = new ArrayList<>();
        QueryBuilder query_timestamp = null;
        String from = null;
        String to = null;

        String gl2_remote_ip;
        String gl2_remote_port;
        String gl2_source_node;
        String source;
        String message;
        String gl2_source_input;
        String timestamp = null;

        for (int i = 0; i < arry_spl.length; i++) {

            String x = arry_spl[i];
            if (x.contains("timestamp")) {
                timestamp = x.trim().split("=")[1].trim();
                String from_t = timestamp.split("_")[0], to_t = timestamp.split("_")[1];
                from = from_t.substring(0, from_t.length() - 1).replace('T', ' ');
                to = to_t.substring(0, to_t.length() - 1).replace('T', ' ');
                query_timestamp = QueryBuilders.rangeQuery(Es_SearchImpl.TIMESTAMP).from(from).to(to);
                queryBuilderList.add(query_timestamp);
            }

            if (x.contains("gl2_remote_ip")) {
                gl2_remote_ip = x.trim();
                String target = gl2_remote_ip.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(gl2_remote_ip, target));
                continue;
            }
            if (x.contains("gl2_remote_port")) {
                gl2_remote_port = x.trim();
                String target = gl2_remote_port.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(gl2_remote_port, target));
                continue;
            }
            if (x.contains("gl2_source_node")) {
                gl2_source_node = x.trim();
                String target = gl2_source_node.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(gl2_source_node, target));
                continue;
            }
            if (x.contains("source")) {
                source = x.trim();
                String target = source.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(source, target));
                continue;
            }
            if (x.contains("message")) {
                message = x.trim();
                String target = message.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(message, target));
                continue;
            }
            if (x.contains("gl2_source_input")) {
                gl2_source_input = x.trim();
                String target = gl2_source_input.split(":")[1];
                queryBuilderList.add(QueryBuilders.matchQuery(gl2_source_input, target));
                continue;
            }
        }

        return queryBuilderList;
    }

    private static void getCEPResult(DataStream<Event_d> input, Pattern<Event_d, ?> pattern, final String uid, final String cot, final String start, final String follow) {


        File dir = new File(System.getProperty("user.dir"));
        //临时目录/data/tools/devlop/idea/flink-test/cep
        System.out.println(dir.getPath());
        if (!dir.exists()) {
            dir.mkdir();
        }
        //默认没有uid
        boolean hasUid = false;
        if (!"all".equals(uid)) hasUid = true;
        //默认 没有contains 关键字 with
        boolean hasContains = false;
        if (!"null".equals(cot)) hasContains = true;

        log.info("hasuid :" + hasUid + " uid= " + uid + " hascontains:" + hasContains + "  hascontains= " + cot);
        u_and_c = new ArrayList<>();
        only_u = new ArrayList<>();
        only_c = new ArrayList<>();
        none = new ArrayList<>();

        final boolean finalHasUid = hasUid;
        final boolean finalHasContains = hasContains;
        logEntityList = new ArrayList<>();
        DataStream<LogEntity> result = CEP.pattern(input, pattern).select(
                new PatternSelectFunction<Event_d, LogEntity>() {
                    @Override
                    public LogEntity select(Map<String, Event_d> pattern) throws Exception {
                        LogEntity logEntity = new LogEntity();
                        String index_uid = pattern.get(start).getName().trim();
                        String message = pattern.get(follow).getPrice();
                        if (finalHasUid && index_uid.equals(uid)) {
                            if (finalHasContains && message.contains(cot)) {
                                logEntity.setIndex(index_uid);
                                logEntity.setMessage(message);
                                u_and_c.add(logEntity);
                                log.info("logEntity u and c:" + u_and_c.size() + "  id=" + logEntity.getIndex());
                                return logEntity;
                            } else {
                                logEntity.setIndex(index_uid);
                                logEntity.setMessage(message);
                                only_u.add(logEntity);
                                // log.info("logEntity only_u:" + only_u.size() + "  id=" + logEntity.getIndex());
                                return logEntity;
                            }
                        } else {
                            if (finalHasContains && message.contains(cot)) {
                                logEntity.setIndex(index_uid);
                                logEntity.setMessage(message);
                                only_c.add(logEntity);
                                //log.info("logEntity only_c:" + only_c.size() + "  id=" + logEntity.getIndex());
                                return logEntity;
                            } else {
                                logEntity.setIndex(index_uid);
                                logEntity.setMessage(message);
                                //logEntity = logRepository.save(logEntity);

                                none.add(logEntity);
                                //log.info("logEntity none size :" + none.size() + "  id=" + logEntity.getIndex());
                                return logEntity;
                            }
                        }
                    }
                });

        //result.writeAsText(dir + "/cep_result", FileSystem.WriteMode.OVERWRITE);

        if (hasUid) {
            if (hasContains) {
                logEntityList = u_and_c;
            } else {
                logEntityList = only_u;
            }
        } else {
            if (hasContains) {
                logEntityList = only_c;
            } else {
                logEntityList = none;
            }
        }
//
    }

    private static DataStream<Event_d> dataETL(DataStream<String> datas) {

        DataStream<String> da = datas.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.contains(" JID-") && value.contains("INFO: ")) {
                    return true;
                }
                return false;
            }
        });
        DataStream<Tuple4<String, String, String, String>> t3 = da.map(new MapFunction<String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(String value) throws Exception {
                String[] info = value.split("INFO: ");
                if (info.length == 2) {
                    String[] jid = info[0].split(" JID-");
                    if (jid.length == 2) {
                        // "message":" xxx JID-666666666666666 INFO: (start or SendThirdPartWorker or done ) xxx"
                        return new Tuple4<String, String, String, String>(jid[0], jid[1].trim(), info[1], "");
                    }
                }
                return null;
            }
        });

        //将tuple转换成event
        DataStream<Event_d> input = t3.map(new MapFunction<Tuple4<String, String, String, String>, Event_d>() {
            @Override
            public Event_d map(Tuple4<String, String, String, String> value) throws Exception {
                return new Event_d(value.f0, value.f1, value.f2, value.f3);
            }
        }).keyBy(new KeySelector<Event_d, String>() {

            @Override
            public String getKey(Event_d value) throws Exception {
                //System.out.println(value.getName());
                return value.getName();
            }
        });
        return input;
    }

    //start SendThirdPartWorker done within cont
    private static Pattern<Event_d, ?> definePattern(final String start, final String follow, final String end, Integer within) {
        //cep
        Pattern<Event_d, ?> pattern = Pattern.<Event_d>begin(start).where(
                new FilterFunction<Event_d>() {
                    @Override
                    public boolean filter(Event_d value) throws Exception {
                        return value.getPrice().contains(start);//&& MD5Util.MD5(value.getMd5())==;
                    }
                }).followedBy(follow).where(new FilterFunction<Event_d>() {
            @Override
            public boolean filter(Event_d value) throws Exception {
                return value.getPrice().contains(follow);//&& jidMap.get(value.getName())==value.getName();
            }
        }).followedBy(end).where(new FilterFunction<Event_d>() {
            @Override
            public boolean filter(Event_d value) throws Exception {
                return value.getPrice().contains(end);//&& a;
            }
        }).within(Time.seconds(within));
        return pattern;
    }

}
