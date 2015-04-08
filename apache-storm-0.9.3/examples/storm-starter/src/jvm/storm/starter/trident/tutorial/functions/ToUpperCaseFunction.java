package storm.starter.trident.tutorial.functions;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Function that Just Emits the uppercase text.
 * Created by Parth Satra on 2/17/15.
 */
public class ToUpperCaseFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0).toUpperCase()));
    }
}
