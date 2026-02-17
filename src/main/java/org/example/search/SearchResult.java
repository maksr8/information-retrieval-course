package org.example.search;

import java.util.Set;

public interface SearchResult {
    SearchResult and(SearchResult other);
    SearchResult or(SearchResult other);
    SearchResult not(int totalDocs);

    Set<Integer> toSet();
}
