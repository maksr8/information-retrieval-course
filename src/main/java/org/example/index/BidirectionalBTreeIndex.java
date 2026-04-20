package org.example.index;

import java.util.*;

public class BidirectionalBTreeIndex implements WildcardIndex {
    private static final Object PRESENT = new Object();

    private final BTree<String, Object> directTree;
    private final BTree<String, Object> inverseTree;

    public BidirectionalBTreeIndex(int degree) {
        this.directTree = new BTree<>(degree);
        this.inverseTree = new BTree<>(degree);
    }

    @Override
    public void buildFromDictionary(Collection<String> dictionary) {
        for (String term : dictionary) {
            add(term);
        }
    }

    @Override
    public void addTerm(String term) {
        add(term);
    }

    private void add(String term) {
        directTree.put(term, PRESENT);
        String reversedTerm = new StringBuilder(term).reverse().toString();
        inverseTree.put(reversedTerm, PRESENT);
    }

    @Override
    public List<String> getPossibleTerms(String wildcardPattern) {
        String[] parts = wildcardPattern.split("\\*", -1);

        if (parts.length == 1) {
            return directTree.get(parts[0]) != null ? List.of(parts[0]) : Collections.emptyList();
        }

        String prefix = parts[0];
        String suffix = parts[parts.length - 1];

        boolean hasPrefix = !prefix.isEmpty();
        boolean hasSuffix = !suffix.isEmpty();

        if (hasPrefix && hasSuffix) { //a*x*b
            List<String> preTerms = getTermsByPrefix(prefix);
            List<String> sufTerms = getTermsBySuffix(suffix);
            Set<String> validSuffixTerms = new HashSet<>(sufTerms);
            return preTerms.stream()
                    .filter(validSuffixTerms::contains)
                    .toList();
        } else if (hasPrefix) { // a*x*
            return getTermsByPrefix(prefix);
        } else if (hasSuffix) { // *x*b
            return getTermsBySuffix(suffix);
        } else { // *x*
            System.err.println("WARN: expensive wildcard query (" + wildcardPattern + "). Use k-gram index for this");
            return directTree.rangeSearch("", "\uFFFF");
        }
    }

    private List<String> getTermsByPrefix(String prefix) {
        return directTree.rangeSearch(prefix, prefix + '\uFFFF');
    }

    private List<String> getTermsBySuffix(String suffix) {
        String reversedSuffix = new StringBuilder(suffix).reverse().toString();
        List<String> matchingReversedTerms = inverseTree.rangeSearch(reversedSuffix, reversedSuffix + '\uFFFF');

        List<String> normalTerms = new ArrayList<>(matchingReversedTerms.size());
        for (String revTerm : matchingReversedTerms) {
            normalTerms.add(new StringBuilder(revTerm).reverse().toString());
        }
        return normalTerms;
    }
}