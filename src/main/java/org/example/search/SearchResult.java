package org.example.search;

import java.util.List;

public interface SearchResult {
    SearchResult and(SearchResult other);
    SearchResult or(SearchResult other);
    SearchResult not(int totalDocs);

    List<Integer> toList(); //must be sorted
}
