package storm.starter.trident.homework.state;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Updater class that updates the state with the new tweets.
 * Created by Parth Satra on 4/5/15.
 */
public class TopKStateUpdater extends BaseStateUpdater<TopKState> {

    @Override
    public void updateState(TopKState topKState, List<TridentTuple> list, TridentCollector tridentCollector) {
        for(TridentTuple tuple : list) {
            // Gets all the space separated hashtags.
            String hashTags = tuple.getString(0);
            String[] tag = hashTags.split(" ");
            // Creates the list to be added to the state
            List<TopTweet> tweetList = new ArrayList<TopTweet>();
            for(String t : tag) {
                if(t != null && t.trim().length() != 0) {
                    TopTweet tt = new TopTweet(t, 1);
                    tweetList.add(tt);
                }
            }
            // Adds the list to the state.
            topKState.add(tweetList);
        }
    }
}
