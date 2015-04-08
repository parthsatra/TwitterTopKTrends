package storm.starter.trident.homework.state;

import storm.trident.state.State;

import java.io.Serializable;
import java.util.*;

/**
 * This class keeps the state of the sliding window and maintains the top k hashtags at any given time.
 * Created by Parth Satra on 4/5/15.
 */
public class TopKState implements State, Serializable{

    private static final long serialVersionUID = -5883475274183700072L;
    // The topk number
    private int topK;
    // The sliding window size.
    private int windowSize;
    // Topk hashtags list at any given time.
    Queue<TopTweet> heap = new PriorityQueue<TopTweet>();
    // Sliding window which consists of the twweets during that sliding window time
    Queue<List<TopTweet>> slidingWindow = new LinkedList<List<TopTweet>>();
    // A dynamic data structure that maintains the count of all the hashtags in the sliding window.
    Map<String, Long> sketch = new HashMap<String, Long>();

    public TopKState(int topK, int windowSize) {
        this.topK = topK;
        this.windowSize = windowSize;
        heap = new PriorityQueue<TopTweet>(topK);
        slidingWindow = new LinkedList<List<TopTweet>>();
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
    }

    /**
     * Adds the List of hashtags to the state and updates the sliding window.
     * @param items List of tweet hashtags and their frequency.
     */
    public void add(List<TopTweet> items) {
        if(items == null || items.size() == 0) {
            return;
        }

        // Removes the oldest tweet from the sliding window.
        if(slidingWindow.size() == windowSize) {
            List<TopTweet> oldList = slidingWindow.poll();
            // Update the topk list for the removed hashtags with the old tweet.
            for(TopTweet oldTweet : oldList) {
                // Update the sketch to remove the old hashtags
                if(sketch.containsKey(oldTweet.getHashTag())) {
                    long count = sketch.remove(oldTweet.getHashTag());
                    count --;
                    if(count > 0) {
                        sketch.put(oldTweet.getHashTag(), count);
                    }
                    // Update the topk queue as well.
                    if(heap.contains(oldTweet)) {
                        heap.remove(oldTweet);
                        if(count > 0) {
                            heap.add(new TopTweet(oldTweet.getHashTag(), count));
                        }
                    }
                }
            }
        }

        // Adds the hastags of the new tweet into the sliding window.
        slidingWindow.add(items);
        // Update the steck and topk list with the newly added tweet in the sliding window.
        for(TopTweet tweet : items) {
            long count = 1;
            if(sketch.containsKey(tweet.getHashTag())) {
                count = sketch.remove(tweet.getHashTag());
                sketch.put(tweet.getHashTag(), count + 1);
            } else {
                sketch.put(tweet.getHashTag(), count);
            }

            if(heap.contains(tweet)) {
                heap.remove(tweet);
            }
            heap.add(new TopTweet(tweet.getHashTag(), count));
        }

        // Keep only the configured topk hashtag list.
        while(heap.size() > topK) {
            heap.poll();
        }
    }

    /**
     * Returns the topk hashtags at any given time.
     * @return  Array of hashtags and their frequency.
     */
    public TopTweet[] getTopKTweets() {
        return heap.toArray(new TopTweet[0]);
    }

}
