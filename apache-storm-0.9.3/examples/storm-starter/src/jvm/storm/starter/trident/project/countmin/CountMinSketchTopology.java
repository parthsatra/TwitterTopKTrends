package storm.starter.trident.project.countmin;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.project.countmin.state.CountMinSketchStateFactory;
import storm.starter.trident.project.countmin.state.CountMinSketchUpdater;
import storm.starter.trident.project.countmin.state.TopKQuery;
import storm.starter.trident.project.filters.StopWordBloomFilter;
import storm.starter.trident.project.functions.LowerCaseFunction;
import storm.starter.trident.project.functions.ParseTweet;
import storm.starter.trident.project.functions.SentenceBuilder;
import storm.starter.trident.project.spouts.TwitterSampleSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.testing.Split;

/**
 * @author: Preetham MS (pmahish@ncsu.edu)
 */


public class CountMinSketchTopology {

    public static StormTopology buildTopology(String args[], LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();

        // This sets the size of the count-min sketch and the k value for the top-k.
        // In this case the k value is 5.
        int width = 100;
        int depth = 150;
        int seed = 100;
        int k = 5;

        // These are the twitter o-auth keys required to connect to twitter.
        // These need to be present as environment variables in $HOME/.bashrc file.
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

        // Twitter topic of interest
        String[] topicWords = args.clone();

        // Create Twitter's spout
        TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, topicWords);

        CountMinSketchStateFactory factory = new CountMinSketchStateFactory(depth, width, seed, k);
        CountMinSketchUpdater countMinSketchUpdater = new CountMinSketchUpdater();
        StopWordBloomFilter bloomFilter = new StopWordBloomFilter();
        TopKQuery topKQuery = new TopKQuery();

        TridentState countMinDBMS = topology.newStream("tweets", spoutTweets)
                // Parses the tweets out from Twitters JSON response.
                .each(new Fields("tweet"), new ParseTweet(), new Fields("text", "tweetId", "user"))
                        //.each(new Fields("text","tweetId","user"), new PrintFilter("PARSED TWEETS:"))
                // Constructs a sentence containing username and text from the twitter tweets.
                .each(new Fields("text", "tweetId", "user"), new SentenceBuilder(), new Fields("sentence"))
                // Splits the text into individual words.
                .each(new Fields("sentence"), new Split(), new Fields("words"))
                // Filters and converts words into lowercase characters.
                .each(new Fields("words"), new LowerCaseFunction(), new Fields("lowercase_words"))
                // Ignores words with NULL String.
                .each(new Fields("lowercase_words"), new FilterNull())
                // Apply bloom Filter to remaining words to further filter the stop words.
                .each(new Fields("lowercase_words"), bloomFilter)
                // Store the words into count-min sketch and also maintain a top-k priority queue.
                .partitionPersist(factory, new Fields("lowercase_words"), countMinSketchUpdater);


        topology.newDRPCStream("top_K_drpc", drpc)
                .parallelismHint(1)
                // Returns the list of top-k words.
                .stateQuery(countMinDBMS, topKQuery, new Fields("top"))
                .project(new Fields("top"));

        return topology.build();

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(10);

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("top_K", conf, buildTopology(args, drpc));

        while(true){
            // DRPC Call for getting the Top-K words.
            // This prints the top-k words along wth its frequencies present in the count-min sketch.
            System.out.println("DRPC RESULT:" + drpc.execute("top_K_drpc", ""));
            Thread.sleep(3000);
        }

        //cluster.shutdown();
        //drpc.shutdown();
    }
}
