package storm.starter.trident.project.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * This function converts the input String into a lowercase characters.
 * Secondly It also filters words with sie less than 4 letters.
 * This is mainly to avoid words like RT, KCA and so on.
 * Created by Parth Satra on 3/1/15.
 */
public class LowerCaseFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String text = tuple.getString(0);
        String output = " ";
        // Words with less than 4 letters
        if(text.length() < 4) {
            output = " ";
        } else if((text.contains("#") || text.contains("@")) && text.length() < 5) {
            output = " ";
        } else {
            // Converts the remaining words to lower case.
            output = text.toLowerCase();
        }
        collector.emit(new Values(output));
    }
}
