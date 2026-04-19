package org.example.index;

import org.example.search.SearchResult;
import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class KGramIndex implements WildcardIndex {
    private static final String MARKER = " ";

    private final int k;
    private final String padding;
    private final InvertedIndex invertedIndex;
    private final BTree<String, List<String>> kgramTree;
    private boolean isSealed = false;

    public KGramIndex(int k, int btreeDegree) {
        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
        this.k = k;
        this.padding = MARKER.repeat(k - 1);
        this.invertedIndex = new InvertedIndex();
        this.kgramTree = new BTree<>(btreeDegree);
    }

    @Override
    public void add(int docId, String term) {
        boolean isNewTerm = !invertedIndex.hasTerm(term);
        invertedIndex.add(docId, term);
        if (!isNewTerm) {
            return;
        }

        String paddedTerm = padding + term + padding;
        for (int i = 0; i <= paddedTerm.length() - k; i++) {
            String gram = paddedTerm.substring(i, i + k);
            addGramToTree(gram, term);
        }
    }

    private void addGramToTree(String gram, String term) {
        List<String> terms = kgramTree.get(gram);
        if (terms == null) {
            terms = new ArrayList<>();
            kgramTree.put(gram, terms);
        }
        terms.add(term);
    }

    @Override
    public List<String> getPossibleTerms(String wildcardPattern) {
        String[] parts = wildcardPattern.split("\\*", -1);

        if (parts.length == 1) {
            return invertedIndex.hasTerm(parts[0]) ? List.of(parts[0]) : Collections.emptyList();
        }

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

        if (extractedGrams.isEmpty()) {
            System.err.println("WARN: query too short for " + k + "-gram index (" + wildcardPattern + "). Performing full dictionary scan.");
            return invertedIndex.getAllTerms();
        }

        List<String> possibleTerms = null;

        for (String gram : extractedGrams) {
            List<String> termsForGram = kgramTree.get(gram);
            
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

    @Override
    public SearchResult getDocsForTerms(List<String> terms) {
        SearchResult combined = null;
        for (String term : terms) {
            SearchResult res = invertedIndex.search(term);
            if (combined == null) {
                combined = res;
            } else {
                combined = combined.or(res);
            }
        }
        return combined != null ? combined : invertedIndex.search("");
    }

    @Override
    public SearchResult search(String term) { return invertedIndex.search(term); }

    @Override
    public void registerDoc(String docName) { invertedIndex.registerDoc(docName); }

    @Override
    public String getDocName(int docId) { return invertedIndex.getDocName(docId); }

    @Override
    public int getDocCount() { return invertedIndex.getDocCount(); }

    @Override
    public void seal() {
        if (isSealed) return;
        invertedIndex.seal();
        isSealed = true;
    }

    @Override
    public void save(Path path) throws IOException {
        if (!isSealed) seal();
        invertedIndex.save(Path.of(path.toString() + ".inv"));

        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            List<String> allGrams = kgramTree.rangeSearch("", "\uFFFF");
            dos.writeInt(allGrams.size());
            for (String gram : allGrams) {
                dos.writeUTF(gram);
                List<String> terms = kgramTree.get(gram);
                dos.writeInt(terms.size());
                for (String term : terms) {
                    dos.writeUTF(term);
                }
            }
        }
    }

    @Override
    public void load(Path path) throws IOException {
        invertedIndex.load(Path.of(path.toString() + ".inv"));

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int gramCount = dis.readInt();
            for (int i = 0; i < gramCount; i++) {
                String gram = dis.readUTF();
                int termsCount = dis.readInt();
                List<String> terms = new ArrayList<>(termsCount);
                for (int j = 0; j < termsCount; j++) {
                    terms.add(dis.readUTF());
                }
                kgramTree.put(gram, terms);
            }
        }
        this.isSealed = true;
    }
}