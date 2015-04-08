package storm.starter.trident.tutorial.filters;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.regex.Pattern;

/**
 * Created by Parth Satra.
 */
public class RegexFilter extends BaseFilter{
    private final Pattern pattern;

    public RegexFilter(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String author = tridentTuple.getString(0);
        return pattern.matcher(author).matches();
    }
}
