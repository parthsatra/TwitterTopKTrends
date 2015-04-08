package storm.starter.trident.project.countmin.state;

import java.io.Serializable;

/**
 * This class maintains a word and its corresponding frequency.
 * Created by Parth Satra on 3/1/15.
 */
public class TopTweet implements Serializable, Comparable<TopTweet> {

    private static final long serialVersionUID = 8567486115087572607L;
    private String text;
    private long count;

    public TopTweet() {

    }

    public TopTweet(String text, long count) {
        this.text = text;
        this.count = count;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopTweet)) return false;

        TopTweet topTweet = (TopTweet) o;

        if (text != null ? !text.equals(topTweet.text) : topTweet.text != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = text != null ? text.hashCode() : 0;
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }

    /**
     * Compares the object passed depending on the frequency attached to it.
     * The object with higher frequency is given greater priority.
     */
    @Override
    public int compareTo(TopTweet o) {
        if(getCount() > o.getCount()) {
            return 1;
        }
        return -1;
    }
}
