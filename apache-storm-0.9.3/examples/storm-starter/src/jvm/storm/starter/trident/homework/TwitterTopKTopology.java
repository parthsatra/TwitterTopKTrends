package storm.starter.trident.homework;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.homework.function.LowerCaseFunction;
import storm.starter.trident.homework.function.ParseTweet;
import storm.starter.trident.homework.spouts.TwitterSampleSpout;
import storm.starter.trident.homework.state.TopKQuery;
import storm.starter.trident.homework.state.TopKStateFactory;
import storm.starter.trident.homework.state.TopKStateUpdater;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;

import java.util.Arrays;

/**
 * Main class that creates the Storm Topology and DRPC Client to query for the topk hashtags from twitter stream.
 * Created by Parth Satra on 4/5/15.
 */
public class TwitterTopKTopology {
    public static StormTopology buildTopology(String args[], LocalDRPC drpc) {

        TridentTopology topology = new TridentTopology();

        // The window size represents the "time" that we consider for the sliding window.
        // In this case we consider the time to be number of tweets as the sliding window.
        int windowSize = 1000;

        // This field represents the number of topk hashtags that we need to find. The default value is 5.
        int topk = 5;

        // These are the twitter o-auth keys required to connect to twitter.
        // These need to be present as environment variables in $HOME/.bashrc file.
        String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
        String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
        String accessToken = System.getenv("TWITTER_ACCESS_TOKEN");
        String accessTokenSecret = System.getenv("TWITTER_ACCESS_TOKEN_SECRET");

        // Handling the arguments passed as input to dynamically set the topk argument, window size and also filter the twitter api.
        String[] topicWords = new String[0];
        if(args != null && args.length > 0) {
            boolean isTopk = false;
            try {
                topk = Integer.parseInt(args[0]);
                if(topk <= 0) {
                    System.out.println("The topK is invalid and hence running with default value");
                    topk = 5;
                }
                isTopk = true;
                windowSize = Integer.parseInt(args[1]);
                if(windowSize <= 0 || topk > windowSize) {
                    System.out.println("The input for window size is incorrect. Hence running with default values");
                    topk = 5;
                    windowSize = 1000;
                }
            } catch(NumberFormatException e) {
                // Twitter topic of interest
                if(isTopk) {
                    topicWords = Arrays.copyOfRange(args, 1, args.length);
                } else {
                    topicWords = args.clone();
                }
            }
            topicWords = Arrays.copyOfRange(args, 2, args.length);
        }

        // Create Twitter's spout as provided by Apache as a part of open source license
        TwitterSampleSpout spoutTweets = new TwitterSampleSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, topicWords);

        // Creates a state that computes the topk hashtags as the tweets are fetched.
        TopKStateFactory factory = new TopKStateFactory(topk, windowSize);
        // Creates an updater that is used to update the tweets,
        TopKStateUpdater updater = new TopKStateUpdater();
        // Creates a query for querying the topk tweets at any time.
        TopKQuery topKQuery = new TopKQuery();

        TridentState topKDBMS = topology.newStream("tweets", spoutTweets)
                // Parses the tweets out from Twitters JSON response and returns the hashtags.
                .each(new Fields("tweet"), new ParseTweet(), new Fields("hashtags"))
                // Normalizes the hashtags and filters out hashtags in english and converts everything to lowercase.
                // Thus making #HAPPY and #happy as the same.
                .each(new Fields("hashtags"), new LowerCaseFunction(), new Fields("lowercase_words"))
                // This filters the tweets with no hashtags.
                .each(new Fields("lowercase_words"), new FilterNull())
                // Store the hashtags into the created state and also maintain a top-k priority queue.
                .partitionPersist(factory, new Fields("lowercase_words"), updater);


        // DRPC request for the topk hashtags
        topology.newDRPCStream("top_K_drpc", drpc)
                .parallelismHint(1)
                // Returns the list of top-k hashtags.
                .stateQuery(topKDBMS, topKQuery, new Fields("top"))
                .project(new Fields("top"));

        return topology.build();

    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(10);

        // Creates a local topology.
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("top_K", conf, buildTopology(args, drpc));

        while(true){
            // DRPC Call for getting the Top-K words.
            // This prints the top-k hashtags along wth its frequencies present in the sliding window.
            System.out.println("DRPC RESULT:" + drpc.execute("top_K_drpc", ""));
            Thread.sleep(3000);
        }

        //cluster.shutdown();
        //drpc.shutdown();
    }
}
