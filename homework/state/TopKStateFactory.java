package storm.starter.trident.homework.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Factory class to create the state.
 * Created by Parth Satra on 4/5/15.
 */
public class TopKStateFactory implements StateFactory {

    protected int topk;
    protected int windowSize;

    public TopKStateFactory(int topk, int windowSize) {
        this.topk = topk;
        this.windowSize = windowSize;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new TopKState(topk, windowSize);
    }

}
