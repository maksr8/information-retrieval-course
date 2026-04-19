package org.example.search;

import org.example.index.SearchIndex;
import org.example.processing.TermNormalizer;
import org.example.processing.Tokenizer;

import java.util.Collections;
import java.util.List;

public class BiWordSearchEngine {
    private final SearchIndex index;
    private final Tokenizer tokenizer;
    private final TermNormalizer normalizer;

    public BiWordSearchEngine(SearchIndex index, Tokenizer tokenizer, TermNormalizer normalizer) {
        this.index = index;
        this.tokenizer = tokenizer;
        this.normalizer = normalizer;
    }

    public List<Integer> searchPhrase(String phrase) {
        if (phrase == null || phrase.isBlank()) return Collections.emptyList();

        List<String> normalizedTokens = tokenizer.tokenize(phrase)
                .map(normalizer::normalize)
                .filter(term -> term != null && !term.isBlank())
                .toList();

        if (normalizedTokens.isEmpty())
            return Collections.emptyList();

        if (normalizedTokens.size() == 1) {
            System.out.println("Warning: biword search is not effective for single-term queries");
            return Collections.emptyList();
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

        return combinedResult == null ? Collections.emptyList() : combinedResult.toList();
    }
}