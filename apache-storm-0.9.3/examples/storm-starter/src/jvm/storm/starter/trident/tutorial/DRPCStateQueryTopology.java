package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.tutorial.spouts.FakeTweetsBatchSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;

import java.io.IOException;

/**
 * Implementing ping event using DRPC Topology
 *
 * Created by Parth Satra on 2/17/15.
 */
public class DRPCStateQueryTopology {
    private static final String DATA_PATH = "data/500_sentences_en.txt";

    public static void main(String args[]) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("state_drpc", conf, buildTopology(drpc));

        for(int i = 0; i < 10; i++) {
            System.out.println("DRPC Result: " + drpc.execute("count-per-actors-event", "dave dave nathan"));
            Thread.sleep(1000);
        }

        System.out.println("STATUS: OK");

        cluster.shutdown();
        drpc.shutdown();
    }

    private static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        TridentTopology topology = new TridentTopology();
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(DATA_PATH);

        TridentState countStateDBMS = topology.newStream("spout", spout)
                .groupBy(new Fields("actor"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        topology.newDRPCStream("count-per-actors-event", drpc)
                .each(new Fields("args"), new SplitFunction(" "), new Fields("split"))
                .stateQuery(countStateDBMS, new Fields("split"), new MapGet(), new Fields("per-actor-count"))
                .each(new Fields("split", "per-actor-count"), new FilterNull())
                .groupBy(new Fields("split"))
                .aggregate(new Fields("per-actor-count"), new Sum(), new Fields("sum"));

        return topology.build();
    }
}
