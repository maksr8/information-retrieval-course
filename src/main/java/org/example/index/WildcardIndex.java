package org.example.index;

import java.util.Collection;
import java.util.List;

public interface WildcardIndex {
    void buildFromDictionary(Collection<String> dictionary);
    /**
     * Adds a term to the index.
     * DO NOT ADD DUPLICATES!
     * Uniqueness is not checked for performance reasons!
     * @param term
     */
    void addTerm(String term);
    List<String> getPossibleTerms(String wildcardPattern);
}