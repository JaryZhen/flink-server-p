package com.flink.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.IndicesQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by jaryzhen on 6/12/16.
 */

public class Es_SearchImpl {

    public static Es_Utils es_utils = new Es_Utils();
    public static final String TIMESTAMP = "timestamp";

   // public static void main(String[] strings) throws UnknownHostException {
        //searchById("adfada");
       // getESData(100000);
        //rangeQuery("timestamp","2016-06-21 11:00:49.413","2016-06-21 13:33:41.987");
        //searchByQuery();
       // searchByQuery_Count();
        //match_phrase_mutil();
       // scorllQuery_bool();
    //}

    //获取此index的所有数据
    public static List getESData(long size) throws UnknownHostException {

        ArrayList datalist = null;
        System.out.println("start searching.....！");
        Date begin = new Date();
        Client client = es_utils.startupClient();

        SearchResponse scrollResponse = client.prepareSearch(es_utils.INDEX_01)
                .setSearchType(SearchType.SCAN).setSize(Es_Utils.SCROLLSIZE).setScroll(TimeValue.timeValueMinutes(1))
                .execute().actionGet();
        long count = scrollResponse.getHits().getTotalHits();//第一次不返回数据
        if (size > count) size = count;
        System.out.println("total=" + count + " and logsize = " + size);
        datalist = new ArrayList<>();
        for (int i = 0, sum = 0; sum < size; i++) {
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            sum += scrollResponse.getHits().hits().length;
//             System.out.println(scrollResponse.toString());
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            datalist.addAll(toJSON(scrollResponse.toString()));
            System.out.println("total date=" + count + "  already searched=" + sum);        }

        System.out.println("take total time: " + dataComt(new Date().getTime(), begin.getTime()));
        es_utils.shutDownClient();
        return datalist;
    }

    //----------------------term match match_phrase---------------------------------------

    //term是代表完全匹配 即不进行分词器分析，文档中必须包含整个搜索的词汇
    //使用term要确定的是这个字段是否“被分析”(analyzed)，默认的字符串是被分析的。
    public List termQuery(String feild, String target) throws UnknownHostException {
        //QueryBuilder termQuery = QueryBuilders.termQuery("message", "986388d519265bfa106eb99b");
        QueryBuilder termQuery = QueryBuilders.termQuery(feild, target);

        return getDataByScorll(termQuery, 222);
    }

    //比如"宝马多少马力"会被分词为"宝马 多少 马力"

    public static List matchQuery(String feild, String target) throws UnknownHostException {
        //QueryBuilders.matchQuery("name", "葫芦4032娃");
        // QueryBuilder match = QueryBuilders.matchQuery("message", "986388d519265bfa106eb99b");
        QueryBuilder matchQuery = QueryBuilders.matchQuery(feild, target);

        return getDataByScorll(matchQuery, 222);
    }

    //精确匹配所有  同时 包含 "宝马 多少 马力"
    public List matchPhraseQuery(String feild, String target) throws UnknownHostException {
        //slop(1) 表示少匹配一个也ok
        // QueryBuilder match_phrase = QueryBuilders.matchPhraseQuery("gl2_source_input", "56fb85b81e193649b0915588").slop(1);
        QueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(feild, target);//.slop(1);

        return getDataByScorll(matchPhraseQuery, 22);
    }


    // return QueryBuilders.multiMatchQuery("山西省太原市7429街道","home", "now_home" );
    public List multiMatchQuery(String feild, String target) throws UnknownHostException {
        //QueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("56fb85b81e193649b0915588", "gl2_source_input");
        QueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(target, feild);

        return getDataByScorll(multiMatchQuery, 222);
    }


    // 使用模糊查询匹配文档查询。
    public List fuzzyQuery(String feild, String target) throws UnknownHostException {
        QueryBuilder fuzzyQuery = QueryBuilders.fuzzyQuery(feild, target);
        //   QueryBuilder fuzzyQuery= QueryBuilders.fuzzyQuery("name", "葫芦3582");
        return getDataByScorll(fuzzyQuery, 222);
    }


    //-----------------------bool联合查询: must,should,must_not-----------------------------

    //must: 文档必须完全匹配条件
    //should: should下面会带一个以上的条件，至少满足一个条件，这个文档就符合should
    //must_not: 文档必须不匹配条件
    public static List scorllQuery_bool() throws UnknownHostException {

        List<QueryBuilder> queryBuilderList = new ArrayList<>();

        queryBuilderList.add(QueryBuilders.rangeQuery("timestamp").from("2016-06-19 07:51:49.413").to("2016-06-21 07:33:41.987"));
        queryBuilderList.add(QueryBuilders.matchQuery("gl2_source_input", "5767b8aee9335e0d0fa02b37"));
        //qb2构造了一个组合查询（BoolQuery），其对应lucene本身的BooleanQuery，可以通过must、should、mustNot方法对QueryBuilder进行组合，形成多条件查询

        QueryBuilder scorllQuery_bool = QueryBuilders.boolQuery()
                // .must(QueryBuilders.matchQuery("message", "TID"))
                .must(queryBuilderList.get(0))
                .must(queryBuilderList.get(1));


        return getDataByScorll(scorllQuery_bool, 222);
    }


    //----------------------rangeQuery  --------------------------------------------------------

    //QueryBuilders.rangeQuery("attr1").gt("value1")

    public static List rangeQuery(String feild, String from, String to) throws UnknownHostException {
        //"2016-05-17 22:51:49.413"   ----   "2016-05-17 23:33:41.914"
        //QueryBuilders.rangeQuery("timestamp").from("2016-05-17 22:51:49.413").to("2016-05-18 00:33:41.987");
        //feild = timestamp;
        feild = feild.trim();
        QueryBuilder rangeQuery = QueryBuilders.rangeQuery(feild).from(from).to(to);

        return getDataByScorll(rangeQuery, 1000);
    }

    /**
     * 搜索，Query搜索API
     * count查询
     */
    public static void searchByQuery_Count() throws UnknownHostException {
        Client client = es_utils.startupClient();

        long countByCount = client.count(
                new CountRequest(es_utils.INDEX_01).types(es_utils.INDEX_TYPE)
        )
                .actionGet()
                .getCount();

        //预准备
        long countByPrepareCount = client.prepareCount(es_utils.INDEX_01)
                .setTypes(es_utils.INDEX_TYPE)
                .setQuery(QueryBuilders.termQuery("message", "startw"))
                .execute()
                .actionGet()
                .getCount();
        System.out.println("searchByQuery_Count<{}>: " + countByPrepareCount);
    }


    // 以Scorll 的形式获取数据
    public static List getDataByScorll(QueryBuilder queryBuilder, long size) throws UnknownHostException {

        ArrayList datalist = null;
        //预准备执行搜索
        Client client = es_utils.startupClient();

        System.out.println("start searching ...... ");
        Date begin = new Date();

        SearchResponse scrollResponse = client.prepareSearch(es_utils.INDEX_01).setQuery(queryBuilder)
                .setSearchType(SearchType.SCAN).setSize(Es_Utils.SCROLLSIZE).setScroll(TimeValue.timeValueMinutes(1))
                .execute().actionGet();
        // System.out.println(scrollResponse.toString());
        long count = scrollResponse.getHits().getTotalHits();//第一次不返回数据
        if (size > count) size = count;
        System.out.println("total=" + count + " and logsize = " + size);
        datalist = new ArrayList<>();
        for (int i = 0, sum = 0; sum < size; i++) {
            scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(10))
                    .execute().actionGet();
            sum += scrollResponse.getHits().hits().length;
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            //  System.out.println(scrollResponse.toString());
            datalist.addAll(toJSON(scrollResponse.toString()));
            System.out.println("total date=" + count + "  already searched=" + sum);
        }
        System.out.println("take total time: " + dataComt(new Date().getTime(), begin.getTime()));
        es_utils.shutDownClient();
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
//                System.out.println(i);
//                System.out.println(a[i].toString());
                JSONObject o = JSON.parseObject(a[i].toString());
                JSONObject source = o.getJSONObject("_source");
                String mesg = source.getString("message");
                datalist.add(mesg);
            }
        }
        return datalist;
    }

    // final long ti  =new Date().getTime();
    public static String dataComt(long current, long last) {
        long c = (current - last) / 1000;
        return c + "s ";
    }


    //-----------------search-----------------------------------------

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * constant score query
     * 另一个查询和查询,包裹查询只返回一个常数分数等于提高每个文档的查询。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder constantScoreQuery() {
        /*return // Using with Filters
                QueryBuilders.constantScoreQuery(FilterBuilders.termFilter("name", "kimchy"))
                        .boost(2.0f);*/

        // With Queries
        return QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name", "葫芦3033娃"))
                .boost(2.0f);
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * disjunction max query
     * 一个生成的子查询文件产生的联合查询，
     * 而且每个分数的文件具有最高得分文件的任何子查询产生的，
     * 再加上打破平手的增加任何额外的匹配的子查询。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder disMaxQuery() {
        return QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("name", "kimchy"))          // Your queries
                .add(QueryBuilders.termQuery("name", "elasticsearch"))   // Your queries
                .boost(1.2f)
                .tieBreaker(0.7f);
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * has child / has parent
     * 父或者子的文档查询
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder hasChildQuery() {
        return // Has Child
                QueryBuilders.hasChildQuery("blog_tag",
                        QueryBuilders.termQuery("tag", "something"));

        // Has Parent
        /*return QueryBuilders.hasParentQuery("blog",
                QueryBuilders.termQuery("tag","something"));*/
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * matchall query
     * 查询匹配所有文件。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder matchAllQuery() {
        return QueryBuilders.matchAllQuery();
    }

    /**
     * TODO NotSolved
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * more like this (field) query (mlt and mlt_field)
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder moreLikeThisQuery() {
        // mlt Query
        QueryBuilders.moreLikeThisQuery("home", "now_home") // Fields
                .likeText("山西省太原市7429街道")                 // Text
                .minTermFreq(1)                                 // Ignore Threshold
                .maxQueryTerms(12);                             // Max num of Terms
        // in generated queries

        // mlt_field Query
        return QueryBuilders.moreLikeThisQuery("home")              // Only on single field
                .likeText("山西省太原市7429街道")
                .minTermFreq(1)
                .maxQueryTerms(12);
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * prefix query
     * 包含与查询相匹配的文档指定的前缀。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder prefixQuery() {
        return QueryBuilders.prefixQuery("name", "葫芦31");
    }

    /**
     * TODO NotSolved
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * querystring query
     * 　　查询解析查询字符串,并运行它。有两种模式,这种经营。
     * 第一,当没有添加字段(使用{ @link QueryStringQueryBuilder #字段(String)},将运行查询一次,非字段前缀
     * 　　将使用{ @link QueryStringQueryBuilder # defaultField(字符串)}。
     * 第二,当一个或多个字段
     * 　　(使用{ @link QueryStringQueryBuilder #字段(字符串)}),将运行提供的解析查询字段,并结合
     * 　　他们使用DisMax或者一个普通的布尔查询(参见{ @link QueryStringQueryBuilder # useDisMax(布尔)})。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder queryString() {
        return QueryBuilders.queryStringQuery("+kimchy -elasticsearch");
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * range query
     * 查询相匹配的文档在一个范围。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder rangeQuery() {
        return QueryBuilders
                .rangeQuery("name")
                .from("葫芦1000娃")
                .to("葫芦3000娃")
                .includeLower(true)     //包括下界
                .includeUpper(false); //包括上界
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * span queries (first, near, not, or, term)
     * 跨度查询
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder spanQueries() {
        // Span First
        QueryBuilders.spanFirstQuery(
                QueryBuilders.spanTermQuery("name", "葫芦580娃"),  // Query
                30000                                             // Max查询范围的结束位置
        );

        // Span Near TODO NotSolved
        QueryBuilders.spanNearQuery()
                .clause(QueryBuilders.spanTermQuery("name", "葫芦580娃")) // Span Term Queries
                .clause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"))
                .slop(30000)                                               // Slop factor
                .inOrder(false)
                .collectPayloads(false);

        // Span Not TODO NotSolved
        QueryBuilders.spanNotQuery()
                .include(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
                .exclude(QueryBuilders.spanTermQuery("home", "山西省太原市2552街道"));

        // Span Or TODO NotSolved
        return QueryBuilders.spanOrQuery()
                .clause(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"));

        // Span Term
        //return QueryBuilders.spanTermQuery("name", "葫芦580娃");
    }


    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * terms query
     * 一个查询相匹配的多个value
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static TermsQueryBuilder termsQuery() {
        return QueryBuilders.termsQuery("name", // field
                "葫芦580娃", "葫芦3812娃").minimumShouldMatch(String.valueOf(1));                // values
        // 设置最小数量的匹配提供了条件。默认为1。
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * wildcard query
     * 　　实现了通配符搜索查询。支持通配符* < /tt>,<tt>
     * 　　匹配任何字符序列(包括空),<tt> ? < /tt>,
     * 　　匹配任何单个的字符。注意该查询可以缓慢,因为它
     * 　　许多方面需要遍历。为了防止WildcardQueries极其缓慢。
     * 　　一个通配符词不应该从一个通配符* < /tt>或<tt>
     * 　　< /tt> <tt> ?。
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder wildcardQuery() {
        return QueryBuilders.wildcardQuery("name", "葫芦*2娃");
    }

    /**
     * TODO NotSolved
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * nested query
     * 嵌套查询
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static QueryBuilder nestedQuery() {
        return QueryBuilders.nestedQuery("location",               // Path
                QueryBuilders.boolQuery()                      // Your query
                        .must(QueryBuilders.matchQuery("location.lat", 0.962590433140581))
                        .must(QueryBuilders.rangeQuery("location.lon").lt(0.00000000000000000003))
        )
                .scoreMode("total");                  // max, total, avg or none
    }

    /**
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * indices query
     * 索引查询
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    protected static IndicesQueryBuilder indicesQuery() {
        // Using another query when no match for the main one
        QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("name", "葫芦3812娃"),
                Es_Utils.INDEX_01, "index2"
        )       //设置查询索引上执行时使用不匹配指数
                .noMatchQuery(QueryBuilders.termQuery("age", "葫芦3812娃"));


        // Using all (match all) or none (match no documents)
        return QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("name", "葫芦3812娃"),
                Es_Utils.INDEX_01, "index2"
        )      // 设置不匹配查询,可以是 all 或者 none
                .noMatchQuery("none");
    }


//
//    private static void searchTest(QueryBuilder queryBuilder) {
//        //预准备执行搜索
//        Es_Utils.client.prepareSearch(Es_Utils.INDEX_DEMO_01)
//                .setTypes(Es_Utils.INDEX_DEMO_01_MAPPING)
//                .setQuery(queryBuilder)
//                .setFrom(0).setSize(20).setExplain(true)
//                .execute()
//                //注册监听事件
//                .addListener(new ActionListener<SearchResponse>() {
//                    @Override
//                    public void onResponse(SearchResponse searchResponse) {
//                        Es_Utils.writeSearchResponse(searchResponse);
//                    }
//
//                    @Override
//                    public void onFailure(Throwable e) {
//
//                    }
//                });
//    }


}