package org.example.search;

import org.example.index.SearchIndex;
import org.example.processing.TermNormalizer;
import org.example.processing.Tokenizer;

import java.util.List;
import java.util.Set;

public class BiWordSearchEngine {
    private final SearchIndex index;
    private final Tokenizer tokenizer;
    private final TermNormalizer normalizer;

    public BiWordSearchEngine(SearchIndex index, Tokenizer tokenizer, TermNormalizer normalizer) {
        this.index = index;
        this.tokenizer = tokenizer;
        this.normalizer = normalizer;
    }

    public Set<Integer> searchPhrase(String phrase) {
        if (phrase == null || phrase.isBlank()) return Set.of();

        List<String> normalizedTokens = tokenizer.tokenize(phrase)
                .map(normalizer::normalize)
                .filter(term -> term != null && !term.isBlank())
                .toList();

        if (normalizedTokens.isEmpty()) return Set.of();

        if (normalizedTokens.size() == 1) {
            System.out.println("Warning: Bi-word search is not effective for single-term queries");
            return Set.of();
        }

        SearchResult combinedResult = null;

        for (int i = 0; i < normalizedTokens.size() - 1; i++) {
            String biWord = normalizedTokens.get(i) + " " + normalizedTokens.get(i + 1);
            SearchResult currentResult = index.search(biWord);

            if (combinedResult == null) {
                combinedResult = currentResult;
            } else {
                combinedResult = combinedResult.and(currentResult);
            }
        }

        return combinedResult == null ? Set.of() : combinedResult.toSet();
    }
}