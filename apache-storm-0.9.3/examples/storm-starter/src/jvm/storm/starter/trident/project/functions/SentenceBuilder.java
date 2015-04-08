package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * SentenceBuilder can be used to create a sentence containing the user who tweeted and the tweet text.
 * It returns a field with the name sentence.
 * Created by Parth Satra on 2/28/15.
 */
public class SentenceBuilder extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // Returns the sentence in the form "User: Tweet Text"
        String sentence = tuple.getString(2) + ": " + tuple.getString(0);

        collector.emit(new Values(sentence));
    }
}
