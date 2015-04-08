package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.tutorial.filters.RegexFilter;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.trident.TridentTopology;

import java.io.IOException;

/**
 * Implementing ping event using DRPC Topology
 *
 * Created by Parth Satra on 2/17/15.
 */
public class DRPCPingFilterTopology {
    public static void main(String args[]) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("pingpong_drpc", conf, buildTopology(drpc));

        for(int i = 0; i < 10; i++) {
            System.out.println("DRPC Result: " + drpc.execute("ping-event", "ping pong ping"));
            Thread.sleep(1000);
        }

        System.out.println("STATUS: OK");

        cluster.shutdown();
        drpc.shutdown();
    }

    private static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        TridentTopology topology = new TridentTopology();

        topology.newDRPCStream("ping-event", drpc)
                .each(new Fields("args"), new SplitFunction(" "), new Fields("reply"))
                .each(new Fields("reply"), new RegexFilter("ping"))
                .project(new Fields("reply"));

        return topology.build();
    }
}
