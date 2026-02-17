package org.example.search;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SetResult implements SearchResult {
    private final Set<Integer> set;

    public SetResult(Set<Integer> set) {
        this.set = set;
    }

    @Override
    public SearchResult and(SearchResult other) {
        Set<Integer> result = new HashSet<>(this.set);
        result.retainAll(((SetResult) other).set);
        return new SetResult(result);
    }

    @Override
    public SearchResult or(SearchResult other) {
        Set<Integer> result = new HashSet<>(this.set);
        result.addAll(((SetResult) other).set);
        return new SetResult(result);
    }

    @Override
    public SearchResult not(int totalDocs) {
        Set<Integer> all = IntStream.range(0, totalDocs).boxed().collect(Collectors.toSet());
        all.removeAll(this.set);
        return new SetResult(all);
    }

    @Override
    public Set<Integer> toSet() {
        return set;
    }
}
