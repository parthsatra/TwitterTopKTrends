package storm.starter.trident.project;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.ClientFactory.NodeClient;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import com.github.tlrx.elasticsearch.test.EsSetup;
import org.apache.commons.lang.StringEscapeUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import storm.starter.trident.project.functions.*;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;

import java.io.IOException;
import java.util.Arrays;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteIndex;

// Local spouts, functions and filters


/**
 * This topology shows how to build ElasticSearch engine for a stream made of
 * fake tweets and how to query it, using DRPC calls.
 * This example should be intended as
 * an example of {@link TridentState} custom implementation.
 * Modified by YOUR NAME from original author:
 *
 * @author Nagiza Samatova (samatova@csc.ncsu.edu)
 * @author Preetham Srinath (pmahish@ncsu.edu)
 */
public class RealTimeElasticSearchTopology {

    public static StormTopology buildTopology(String[] args, Settings settings, LocalDRPC drpc)
            throws IOException {

        TridentTopology topology = new TridentTopology();

        ESIndexMapState.Factory<Tweet> stateFactory = ESIndexMapState
                .nonTransactional(new ClientFactory.NodeClient(settings.getAsMap()), Tweet.class);

        TridentState staticState = topology
                .newStaticState(new ESIndexState.Factory<Tweet>(
                        new NodeClient(settings.getAsMap()), Tweet.class));

        /**
         * Create spout(s)
         **/
        // Twitter's account credentials passed as args
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

        // Twitter topic of interest
        String[] arguments = args.clone();
        String[] topicWords = Arrays.copyOfRange(arguments, 0, arguments.length);

        // Create Twitter's spout
        TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
        							accessToken, accessTokenSecret, topicWords);

        /***
         * Here is the useful spout for debugging as it could be of finite size
         * Change setCycle(false) to setCycle(true) if
         * continuous stream of tweets is needed
         **/
        /**
        FixedBatchSpout spoutFixedBatch = new FixedBatchSpout(new Fields("sentence"), 6,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("how many apples can you eat"),
                new Values("apples apples man moon man"),
                new Values("to be or not to be the person"));
        spoutFixedBatch.setCycle(true);
        **/

        /**
         * Read a stream of tweets, parse and extract
         * only the text, and its id. Store the stream
         * using the {@link ElasticSearchState} implementation for Trident.
         * NOTE: Commented lines are for debugging only
         * TO DO FOR PROJECT 2: PART B:
         *    1. Write a spout that ingests real text data stream:
         * 		 You are allowed to utilize TweeterSampleSpout provided (DEAFULT), or
         * 		 you may consider writing a spout that ingests Wikipedia updates
         *		 or any other text stream (e.g., using Google APIs to bring a synopsis of a URL web-page
         *		 for a stream of URL web-pages).
         *	  2. Write proper support/utility functions to prepare your text data
         * 		 for passing to the ES stateFactory: e.g., WikiUpdatesDocumentBuilder(),
         *		 GoogleWebURLSynopsisBuilder().
         */
        /**
        topology.newStream("tweets", spoutFixedBatch)
                //.each(new Fields("sentence"), new Print("sentence field"))
                .parallelismHint(1)
                .each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
                .each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
                        //.each(new Fields("id"), new Print("id field"))
                .groupBy(new Fields("index", "type", "id"))
                .persistentAggregate(stateFactory, new Fields("document"), new TweetBuilder(), new Fields("tweet"))

                        //.each(new Fields("tweet"), new Print("tweet field"))
                .parallelismHint(1)
        ;**/

        /***
         * If you want to use TweeterSampleSpout instead of FixedBatchSpout
         * you will need to write a function SentenceBuilder()
         * to reuse the aforementioned code for populating ES index with tweets
         * using the code that looks like this:
         **/
         topology.newStream("tweets", spoutTweets)
         .parallelismHint(1)
         .each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
         //.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
         .each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
         .each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
         .each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
         //.each(new Fields("id"), new Print("id field"))
         .groupBy(new Fields("index", "type", "id"))
         .persistentAggregate(stateFactory,  new Fields("document"), new TweetBuilder(), new Fields("tweet"))
         //.each(new Fields("tweet"), new Print("tweet field"))
         .parallelismHint(1)
         ;


        /**
         * Now use a DRPC stream to query the state where the tweets are stored.
         * CRITICAL: DO NOT CHANGE "query", "indicies", "types"
         * WHY: QuerySearchIndexQuery() has hard-coded these tags in its code:
         *     https://github.com/fhussonnois/storm-trident-elasticsearch/blob/13bd8203503a81754dc2a421accff216b665a11d/src/main/java/com/github/fhuss/storm/elasticsearch/state/QuerySearchIndexQuery.java
         */

        topology
                .newDRPCStream("search_event", drpc)
                .each(new Fields("args"), new ExtractSearchArgs(), new Fields("query", "indices", "types"))
                .groupBy(new Fields("query", "indices", "types"))
                .stateQuery(staticState, new Fields("query", "indices", "types"), new QuerySearchIndexQuery(), new Fields("tweet"))
                .each(new Fields("tweet"), new FilterNull())
                .each(new Fields("tweet"), new CreateJson(), new Fields("json"))
                .project(new Fields("json"))
        ;


        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        // Specify the name and the type of ES index
        String index_name = new String();
        String index_type = new String();
        index_name = "my_index";
        index_type = "my_type";


        /**
         * Configure local cluster and local DRPC
         ***/
        Config conf = new Config();
        conf.setMaxSpoutPending(10);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

        /**
         * Configure ElasticSearch related information
         * Make sure that elasticsearch (ES) server is up and running
         * Check README.md file on how to install and run ES
         * Note that you can check if index exists and/or was created
         * with curl 'localhost:9200/_cat/indices?v'
         * and type: curl ‘http://127.0.0.1:9200/my_index/_mapping?pretty=1’
         * IMPORTANT: DO NOT CHANGE PORT TO 9200. IT MUST BE 9300 in code below.
         **/
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", "elasticsearch")
                .put("storm.elasticsearch.hosts", "127.0.0.1:9300")
                .build();

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        /** If you need more options, you can check test code:
         * https://github.com/tlrx/elasticsearch-test/blob/master/src/test/java/com/github/tlrx/elasticsearch/test/EsSetupTest.java
         * Remove else{} stmt if you do not want index to be deleted but rather updated
         * NOTE: Index files are stored by default under $HOME/elasticsearch-1.4.2/data/elasticsearch/nodes/0/indices
         **/
        EsSetup esSetup = new EsSetup(client);
        if (!esSetup.exists(index_name)) {
            esSetup.execute(createIndex(index_name));
        } else {
            esSetup.execute(deleteIndex(index_name));
            esSetup.execute(createIndex(index_name));
        }

        cluster.submitTopology("state_drpc", conf, buildTopology(args, settings, drpc));

        System.out.println("STARTING DRPC QUERY PROCESSING");

        /***
         * TO DO FOR PROJECT 2: PART B:
         *   3. Investigate Java APIs for ElasticSearch for various types of queries:
         *		http://www.elasticsearch.com/guide/en/elasticsearch/client/java-api/current/index.html
         *   4. Create at least 3 different query types besides termQuery() below
         *		and test them with the DRPC execute().
         *	    Example queries: matchQuery(), multiMatchQuery(), fuzzyQuery(), etc.
         *   5. Can you thing of the type of indexing technologies ES might be using
         *		to support efficient query processing for such query types:
         *		e.g., fuzzyQuery() vs. boolQuery().
         ***/
        String query1 = QueryBuilders.termQuery("text", "love").buildAsBytes().toUtf8();
        /**
         * This is a prefix query which searches in text if any word starts with word 'love'
         */
        String query2 = QueryBuilders.prefixQuery("text", "love").buildAsBytes().toUtf8();
        /**
         * This is a boolean query which will return text containing word 'hate' but not containing word 'love'
         */
        String query3 = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("text", "hate"))
                .mustNot(QueryBuilders.matchQuery("text", "love"))
                .buildAsBytes().toUtf8();
        /**
         * This is a match query which tries to exactly match the word 'hate' to be present in the text.
         */
        String query4 = QueryBuilders.matchQuery("text","hate").buildAsBytes().toUtf8();

        String drpcResult;
        for (int i = 0; i < 5; i++) {
            drpcResult = drpc.execute("search_event", query1 + " " + index_name + " " + index_type);
            System.out.println("DRPC RESULT TERM QUERY: " + StringEscapeUtils.unescapeJava(drpcResult));
            drpcResult = drpc.execute("search_event", query2 + " " + index_name + " " + index_type);
            System.out.println("DRPC RESULT PREFIX QUERY: " + StringEscapeUtils.unescapeJava(drpcResult));
            drpcResult = drpc.execute("search_event", query3 + " " + index_name + " " + index_type);
            System.out.println("DRPC RESULT BOOLEAN QUERY: " + StringEscapeUtils.unescapeJava(drpcResult));
            drpcResult = drpc.execute("search_event", query4 + " " + index_name + " " + index_type);
            System.out.println("DRPC RESULT MATCH QUERY: " + StringEscapeUtils.unescapeJava(drpcResult));

            Thread.sleep(4000);
        }

        //cluster.shutdown();
        //drpc.shutdown();
        //esSetup.terminate();
    }
}
