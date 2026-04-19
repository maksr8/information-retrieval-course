package org.example.index;

import org.example.search.SearchResult;
import java.util.List;

public interface WildcardIndex extends SearchIndex {
    List<String> getPossibleTerms(String wildcardPattern);
    SearchResult getDocsForTerms(List<String> terms);
}