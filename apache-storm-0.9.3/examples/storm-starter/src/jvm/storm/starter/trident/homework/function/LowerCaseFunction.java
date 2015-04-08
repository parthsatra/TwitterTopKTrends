package storm.starter.trident.homework.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * This function converts the input String into a lowercase characters.
 * Also filters out hashtags which are not in english.
 * Created by Parth Satra on 3/1/15.
 */
public class LowerCaseFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String text = tuple.getString(0);
        // Replace the non english characters with empty string
        text = text.replaceAll("[^a-zA-Z0-9]", "");
        // Converts the hashtag to lowercase
        String output = text.toLowerCase();
        collector.emit(new Values(output));
    }
}
