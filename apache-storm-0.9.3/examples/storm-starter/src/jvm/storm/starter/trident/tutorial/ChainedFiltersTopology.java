package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.tutorial.filters.PrintFilter;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.io.IOException;

/**
 * Created by Parth Satra on 2/17/15.
 */
public class ChainedFiltersTopology {
    // -----------------------------------------
    // Step 2: Specify which file spout
    // should read from (if any).
    // Data path relative to pom.xml file.
    //------------------------------------------
    private static final String DATA_PATH = "data/500_sentences_en.txt";
    private static final String CSV_PATH = "data/20130301.csv.gz";

    public static StormTopology buildTopology() throws IOException {
        //----------------------------------------
        // Step 3: Define input tuple’s fields of interest
        //----------------------------------------
        Fields inputFields = new Fields("id", "text", "actor", "location", "date");
        //--------------------------------------
        // Step 4: Create Trident Spout that emits batches of tuples
        //--------------------------------------
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(DATA_PATH);
        //---------------------------------------------------------
        // Step 5: Define filters to apply to the input fields
        //--------------------------------------------------------
        PrintFilter filter = new PrintFilter();
        //---------------------------------------------------------
        // Step 6: Define functions to operate on the input fields
        //--------------------------------------------------------
        //------------------------------------------------------
        // Step 7: Define output fields produced by the function
        //------------------------------------------------------
        //--------------------------------------
        // Step 8: Create TridentTopology object
        //--------------------------------------
        TridentTopology topology = new TridentTopology();
        //-------------------------------------------------
        // Step 9: Create stream of batches using the spout
        //-------------------------------------------------
        Stream stream = topology.newStream("spout", spout);
        //---------------------------------
        // Step 10: Define what to do with each stream’s batch
        //--------------------------------
        stream.each(new Fields("actor"), new RegexFilter("doug"));
        stream.each(new Fields("actor", "text"), filter);
        // Step 11: Return the built topology
        // ---------------------------------
        return topology.build();
    }

    public static void main(String args[]) throws Exception {
        Config config = new Config();

        if(args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, buildTopology());
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("skeleton", config, buildTopology());
        }
    }
}
