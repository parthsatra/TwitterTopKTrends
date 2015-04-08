package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

import java.io.IOException;

/**
 * Implementing ping event using DRPC Topology
 *
 * Created by Parth Satra on 2/17/15.
 */
public class DRPCCountFilterTopology {
    public static void main(String args[]) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("pingpong_drpc", conf, buildTopology(drpc));

        for(int i = 0; i < 10; i++) {
            System.out.println("DRPC Result: " + drpc.execute("count-event", "advanced advanced algorithms course"));
            Thread.sleep(1000);
        }

        System.out.println("STATUS: OK");

        cluster.shutdown();
        drpc.shutdown();
    }

    private static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        TridentTopology topology = new TridentTopology();

        topology.newDRPCStream("count-event", drpc)
                .each(new Fields("args"), new SplitFunction(" "), new Fields("split"))
                .each(new Fields("split"), new RegexFilter("a.*"))
                .groupBy(new Fields("split"))
                .aggregate(new Count(), new Fields("count"))
                .project(new Fields("split", "count"));

        return topology.build();
    }
}
