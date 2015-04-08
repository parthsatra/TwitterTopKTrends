package storm.starter.trident.homework.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.User;

// Local functions

/**
 * Parses the Json response from twitter using twitter4j
 * @author Parth Satra
 */
public class ParseTweet extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        Status parsed = (Status)tuple.get(0);
        // Fetch all the hashtags from the tweet
        HashtagEntity entities[] = parsed.getHashtagEntities();
        StringBuilder sb = new StringBuilder();
        // Create a space separated String of hashtags from a single tweet.
        for(HashtagEntity entity : entities) {
            String hashtag = entity.getText();
            if(hashtag != null && hashtag.trim().length() != 0) {
                sb.append(hashtag+" ");
            }
        }
        // Emit the constructed string of hashtags
        collector.emit(new Values(sb.toString().trim()));
    }
}
