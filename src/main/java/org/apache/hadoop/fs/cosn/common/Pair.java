package org.apache.hadoop.fs.cosn.common;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A pair of values.
 *
 * @param <First>
 * @param <Second>
 */
public class Pair<First, Second> {
    private First first;
    private Second second;

    public Pair() {
    }

    public Pair(First first, Second second) {
        this.first = first;
        this.second = second;
    }

    @Nullable
    public First getFirst() {
        return first;
    }

    @Nullable
    public Second getSecond() {
        return second;
    }

    public void setFirst(@Nullable First first) {
        this.first = first;
    }

    public void setSecond(@Nullable Second second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(first, pair.first) && Objects.equals(second, pair.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
