package storm.starter.trident.tutorial.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Filter for printing Trident tuples useful for testing and debugging.
 * Created by Parth Satra on 2/17/15.
 */
public class PrintFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        System.out.println(tridentTuple);
        return true;
    }
}
