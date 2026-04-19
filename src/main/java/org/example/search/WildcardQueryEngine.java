package org.example.search;

import org.example.index.WildcardIndex;

import java.util.List;
import java.util.regex.Pattern;

public class WildcardQueryEngine {
    private final WildcardIndex index;

    public WildcardQueryEngine(WildcardIndex index) {
        this.index = index;
    }

    public List<Integer> search(String query) {
        if (query == null || query.isBlank() || !query.contains("*")) {
            return index.search(query).toList();
        }

        String searchPattern = query.toLowerCase();
        List<String> possibleTerms = index.getPossibleTerms(searchPattern);

        // postfiltering
        Pattern regexPattern = buildRegexFromWildcard(searchPattern);
        List<String> validTerms = possibleTerms.stream()
                .filter(term -> regexPattern.matcher(term).matches())
                .toList();

        System.out.println("\nValid terms for wildcard '" + query + "': " + validTerms);
        return index.getDocsForTerms(validTerms).toList();
    }

    private Pattern buildRegexFromWildcard(String wildcardQuery) {
        String regex = "^" + wildcardQuery.replace("*", ".*") + "$";
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }
}