package org.example.search;

import org.example.index.SearchIndex;
import org.example.index.WildcardIndex;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class WildcardQueryEngine {
    private final WildcardIndex wildcardIndex;
    private final SearchIndex documentIndex;

    public WildcardQueryEngine(WildcardIndex wildcardIndex, SearchIndex documentIndex) {
        this.wildcardIndex = wildcardIndex;
        this.documentIndex = documentIndex;
    }

    public List<Integer> search(String query) {
        if (query == null || query.isBlank() || !query.contains("*")) {
            return documentIndex.search(query).toList();
        }

        String searchPattern = query.toLowerCase();
        List<String> possibleTerms = wildcardIndex.getPossibleTerms(searchPattern);

        // postfiltering
        Pattern regexPattern = buildRegexFromWildcard(searchPattern);
        List<String> validTerms = possibleTerms.stream()
                .filter(term -> regexPattern.matcher(term).matches())
                .toList();

        System.out.println("\nValid terms for wildcard '" + query + "': " + validTerms);

        SearchResult combined = null;
        for (String term : validTerms) {
            SearchResult res = documentIndex.search(term);
            if (combined == null) {
                combined = res;
            } else {
                combined = combined.or(res);
            }
        }

        return combined != null ? combined.toList() : Collections.emptyList();
    }

    private Pattern buildRegexFromWildcard(String wildcardQuery) {
        String regex = "^" + wildcardQuery.replace("*", ".*") + "$";
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }
}