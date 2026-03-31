package org.example.search;

import org.example.index.PositionalIndex;
import org.example.processing.TermNormalizer;
import org.example.processing.Tokenizer;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PositionalQueryEngine {
    private final PositionalIndex index;
    private final Tokenizer tokenizer;
    private final TermNormalizer normalizer;

    public PositionalQueryEngine(PositionalIndex index, Tokenizer tokenizer, TermNormalizer normalizer) {
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

        return index.searchPhrase(normalizedTokens);
    }

    public Set<Integer> searchProximity(String query) {
        if (query == null || query.isBlank()) return Set.of();

        Pattern p = Pattern.compile("(.+)\\s+/(\\d+)\\s+(.+)");
        Matcher m = p.matcher(query);
        
        if (m.find()) {
            String term1 = normalizer.normalize(m.group(1).trim());
            int k = Integer.parseInt(m.group(2));
            String term2 = normalizer.normalize(m.group(3).trim());

            if (term1 == null || term2 == null || term1.isBlank() || term2.isBlank()) {
                return Set.of();
            }
            return index.searchProximity(term1, term2, k);
        }
        
        System.err.println("Invalid proximity format. Use 'term1 /k term2'. Query: " + query);
        return Set.of();
    }
}