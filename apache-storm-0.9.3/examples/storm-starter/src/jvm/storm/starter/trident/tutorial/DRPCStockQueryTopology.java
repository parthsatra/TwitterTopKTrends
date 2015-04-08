package storm.starter.trident.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.starter.trident.tutorial.functions.SplitFunction;
import storm.starter.trident.tutorial.spouts.CSVBatchSpout;
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
public class DRPCStockQueryTopology {
    private static final String DATA_PATH = "data/stocks.csv.gz";

    public static void main(String args[]) throws Exception {
        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("stock_drpc", conf, buildTopology(drpc));

        for(int i = 0; i < 10; i++) {
            System.out.println("DRPC RESULT: " + drpc.execute("trade-events", "AAPL GE INTC"));
            Thread.sleep(5000);
        }

        System.out.println("STATUS: OK");

        cluster.shutdown();
        drpc.shutdown();
    }

    private static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        TridentTopology topology = new TridentTopology();
        CSVBatchSpout spoutCSV = new CSVBatchSpout(DATA_PATH, new Fields("date", "symbol", "price", "share"));

        TridentState tradeVolumeDBMS = topology.newStream("spout", spoutCSV)
                .groupBy(new Fields("symbol"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("share"), new Sum(), new Fields("volume"));

        topology.newDRPCStream("trade-events", drpc)
                .each(new Fields("args"), new SplitFunction(" "), new Fields("split"))
                .stateQuery(tradeVolumeDBMS, new Fields("split"), new MapGet(), new Fields("per-stock-count"))
                .each(new Fields("split", "per-stock-count"), new FilterNull())
                .groupBy(new Fields("split"))
                .aggregate(new Fields("per-stock-count"), new Sum(), new Fields("volume"))
                .project(new Fields("split", "volume"));
        return topology.build();
    }
}
