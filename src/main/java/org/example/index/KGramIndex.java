package org.example.index;

import java.util.*;

public class KGramIndex implements WildcardIndex {
    private static final String MARKER = " ";

    private final int k;
    private final String padding;
    private final BTree<String, List<String>> kGramTree;

    public KGramIndex(int k, int btreeDegree) {
        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
        this.k = k;
        this.padding = MARKER.repeat(k - 1);
        this.kGramTree = new BTree<>(btreeDegree);
    }

    @Override
    public void buildFromDictionary(Collection<String> dictionary) {
        for (String term : dictionary) {
            addTerm(term);
        }
    }

    @Override
    public void addTerm(String term) {
        String paddedTerm = padding + term + padding;
        for (int i = 0; i <= paddedTerm.length() - k; i++) {
            String gram = paddedTerm.substring(i, i + k);
            addGramToTree(gram, term);
        }
    }

    private void addGramToTree(String gram, String term) {
        List<String> terms = kGramTree.get(gram);
        if (terms == null) {
            terms = new ArrayList<>();
            kGramTree.put(gram, terms);
        }
        terms.add(term);
    }

    @Override
    public List<String> getPossibleTerms(String wildcardPattern) {
        String[] parts = wildcardPattern.split("\\*", -1);

        if (parts.length == 1) {
            return kGramTree.get(parts[0]) != null ? List.of(parts[0]) : Collections.emptyList();
        }

        List<String> extractedGrams = getGrams(parts);

        if (extractedGrams.isEmpty()) {
            System.err.println("WARN: query too short for " + k + "-gram index (" + wildcardPattern + "). Performing full dictionary scan.");
            return kGramTree.rangeSearch("", "\uFFFF");
        }

        List<String> possibleTerms = null;

        for (String gram : extractedGrams) {
            List<String> termsForGram = kGramTree.get(gram);
            
            if (termsForGram == null || termsForGram.isEmpty()) {
                return Collections.emptyList(); // general return because we need to intersect terms for all grams, if one gram has no terms, the result is empty
            }

            if (possibleTerms == null) {
                possibleTerms = new ArrayList<>(termsForGram);
            } else {
                Set<String> fastLookupSet = new HashSet<>(termsForGram);
                possibleTerms.retainAll(fastLookupSet);
            }

            if (possibleTerms.isEmpty()) {
                return Collections.emptyList();
            }
        }

        return possibleTerms;
    }

    private List<String> getGrams(String[] parts) {
        List<String> extractedGrams = new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.isEmpty()) continue;

            if (i == 0) {
                part = padding + part;
            }
            if (i == parts.length - 1) {
                part = part + padding;
            }

            if (part.length() >= k) {
                for (int j = 0; j <= part.length() - k; j++) {
                    extractedGrams.add(part.substring(j, j + k));
                }
            }
        }
        return extractedGrams;
    }
}