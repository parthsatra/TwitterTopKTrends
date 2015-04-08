package storm.starter.trident.homework.state;

import java.io.Serializable;

/**
 * This class maintains a hashtag and its corresponding frequency.
 * Created by Parth Satra on 3/1/15.
 */
public class TopTweet implements Serializable, Comparable<TopTweet> {

    private static final long serialVersionUID = 8567486115087572607L;
    private String hashTag;
    private long count;

    public TopTweet(String hashTag, long count) {
        this.hashTag = hashTag;
        this.count = count;
    }

    public String getHashTag() {
        return hashTag;
    }

    public void setHashTag(String hashTag) {
        this.hashTag = hashTag;
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

        if (hashTag != null ? !hashTag.equals(topTweet.hashTag) : topTweet.hashTag != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = hashTag != null ? hashTag.hashCode() : 0;
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
