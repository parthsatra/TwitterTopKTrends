//import CountMinSketchState;
package storm.starter.trident.project.countmin.state;

import backtype.storm.tuple.Values;
import org.apache.commons.lang.ArrayUtils;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Query Function to return top-k words at any given time.
 * @author: Parth Satra
 */

public class TopKQuery extends BaseQueryFunction<CountMinSketchState, String> {
    public List<String> batchRetrieve(CountMinSketchState state, List<TridentTuple> inputs) {
        StringBuilder out = new StringBuilder();
        out.append(" ");
        // Fetch all the words from the priority queue.
        TopTweet[] topList = state.getTopKItems();
        // Sort these words in increasing order of frequency.
        Arrays.sort(topList);
        // Reverse the sorted list to get decreasing order.
        ArrayUtils.reverse(topList);
        // Return all these words as a single String.
        for(TopTweet toptweet : topList) {
            out.append("[" + toptweet.getText() + ": " + toptweet.getCount() + "] ");
        }

        List<String> ret = new ArrayList<String>();
        ret.add(out.toString());
        return ret;
    }

    public void execute(TridentTuple tuple, String count, TridentCollector collector) {
        collector.emit(new Values(count));
    }    
}
