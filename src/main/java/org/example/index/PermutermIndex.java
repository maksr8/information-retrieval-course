package org.example.index;

import org.example.search.SearchResult;
import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class PermutermIndex implements WildcardIndex {
    private static final String END_MARKER = " ";
    
    private final InvertedIndex invertedIndex;
    private final BTree<String, String> permutermTree; 
    private boolean isSealed = false;

    public PermutermIndex(int btreeDegree) {
        this.invertedIndex = new InvertedIndex();
        this.permutermTree = new BTree<>(btreeDegree);
    }

    @Override
    public void add(int docId, String term) {
        invertedIndex.add(docId, term);

        String terminalTerm = term + END_MARKER;

        if (permutermTree.get(END_MARKER + term) == null) {
            for (int i = 0; i < terminalTerm.length(); i++) {
                String permutation = terminalTerm.substring(i) + terminalTerm.substring(0, i);
                permutermTree.put(permutation, term);
            }
        }
    }

    @Override
    public List<String> getPossibleTerms(String wildcardPattern) {
        String[] parts = wildcardPattern.split("\\*", -1);

        if (parts.length == 1) {
            return invertedIndex.search(parts[0]).toList().isEmpty() ?
                    Collections.emptyList() : List.of(parts[0]);
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
            List<String> allPerms = permutermTree.rangeSearch("", "\uFFFF");
            dos.writeInt(allPerms.size());
            for (String perm : allPerms) {
                dos.writeUTF(perm);
                dos.writeUTF(permutermTree.get(perm));
            }
        }
    }

    @Override
    public void load(Path path) throws IOException {
        invertedIndex.load(Path.of(path.toString() + ".inv"));
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int size = dis.readInt();
            for (int i = 0; i < size; i++) {
                String perm = dis.readUTF();
                String term = dis.readUTF();
                permutermTree.put(perm, term);
            }
        }
        this.isSealed = true;
    }
}