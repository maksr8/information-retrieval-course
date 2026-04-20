package org.example.index;

import java.util.*;

public class PermutermIndex implements WildcardIndex {
    private static final String END_MARKER = " ";
    
    private final BTree<String, String> permutermTree;

    public PermutermIndex(int btreeDegree) {
        this.permutermTree = new BTree<>(btreeDegree);
    }

    @Override
    public void buildFromDictionary(Collection<String> dictionary) {
        for (String term : dictionary) {
            addTerm(term);
        }
    }

    @Override
    public void addTerm(String term) {
        String terminalTerm = term + END_MARKER;
        for (int i = 0; i < terminalTerm.length(); i++) {
            String permutation = terminalTerm.substring(i) + terminalTerm.substring(0, i);
            permutermTree.put(permutation, term);
        }
    }

    @Override
    public List<String> getPossibleTerms(String wildcardPattern) {
        String[] parts = wildcardPattern.split("\\*", -1);

        if (parts.length == 1) {
            return permutermTree.get(parts[0]) != null ? List.of(parts[0]) : Collections.emptyList();
        }

        String prefixToSearch = "";

        if (parts.length == 2) {
            String X = parts[0];
            String Y = parts[1];

            if (!X.isEmpty() && Y.isEmpty()) { // X*
                prefixToSearch = END_MARKER + X;
            } else if (X.isEmpty() && !Y.isEmpty()) { // *Y
                prefixToSearch = Y + END_MARKER;
            } else if (!X.isEmpty()) { // X*Y
                prefixToSearch = Y + END_MARKER + X;
            }
        } else { // X*Y*Z
            String first = parts[0];
            String last = parts[parts.length - 1];

            if (!first.isEmpty() && !last.isEmpty()) { // X*Y*Z
                prefixToSearch = last + END_MARKER + first; // "Z X"
            } else if (!first.isEmpty()) { // X*Y*
                prefixToSearch = END_MARKER + first; // " X"
            } else if (!last.isEmpty()) { // *Y*Z
                prefixToSearch = last + END_MARKER; // "Z "
            } else { // *Y*
                System.err.println("WARN: expensive wildcard query (" + wildcardPattern + "). Use k-gram index for this");
            }
        }

        List<String> permutations = permutermTree.rangeSearch(prefixToSearch, prefixToSearch + '\uFFFF');

        Set<String> originalTerms = new HashSet<>();
        for (String perm : permutations) {
            String originalTerm = permutermTree.get(perm);
            if (originalTerm != null) {
                originalTerms.add(originalTerm);
            }
        }

        return new ArrayList<>(originalTerms);
    }
}