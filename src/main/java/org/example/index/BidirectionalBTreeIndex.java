package org.example.index;

import org.example.search.SearchResult;
import org.example.search.ListResult;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class BidirectionalBTreeIndex implements WildcardIndex {
    
    private final BTree<String, List<Integer>> directTree;
    private final BTree<String, List<Integer>> inverseTree;
    private final List<String> docNames = new ArrayList<>();

    public BidirectionalBTreeIndex(int degree) {
        this.directTree = new BTree<>(degree);
        this.inverseTree = new BTree<>(degree);
    }

    @Override
    public void add(int docId, String term) {
        List<Integer> directDocs = directTree.get(term);
        if (directDocs == null) {
            directDocs = new ArrayList<>();
            directTree.put(term, directDocs);
        }
        if (directDocs.isEmpty() || directDocs.getLast() != docId) {
            directDocs.add(docId);
        }

        String reversedTerm = new StringBuilder(term).reverse().toString();
        List<Integer> inverseDocs = inverseTree.get(reversedTerm);
        if (inverseDocs == null) {
            inverseDocs = new ArrayList<>();
            inverseTree.put(reversedTerm, inverseDocs);
        }
        if (inverseDocs.isEmpty() || inverseDocs.getLast() != docId) {
            inverseDocs.add(docId);
        }
    }

    @Override
    public SearchResult search(String term) {
        List<Integer> docs = directTree.get(term);
        return new ListResult(docs != null ? docs : Collections.emptyList());
    }

    @Override
    public void registerDoc(String docName) {
        docNames.add(docName);
    }

    @Override
    public String getDocName(int docId) {
        if(docId < 0 || docId >= docNames.size()) {
            throw new IllegalArgumentException("Invalid docId: " + docId + ". Valid range is 0 to " + (docNames.size() - 1));
        }
        return docNames.get(docId);
    }

    @Override
    public int getDocCount() {
        return docNames.size();
    }

    @Override
    public void seal() {
        System.out.println("Bidirectional BTree Index sealed");
    }

    @Override
    public void save(Path path) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            dos.writeInt(docNames.size());
            for (String docName : docNames) {
                dos.writeUTF(docName);
            }

            List<String> allTerms = directTree.rangeSearch("", "\uFFFF");
            dos.writeInt(allTerms.size());

            for (String term : allTerms) {
                dos.writeUTF(term);
                List<Integer> docs = directTree.get(term);
                dos.writeInt(docs.size());
                for (int docId : docs) {
                    dos.writeInt(docId);
                }
            }
        }
    }

    @Override
    public void load(Path path) throws IOException {
        docNames.clear();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            int docCount = dis.readInt();
            for (int i = 0; i < docCount; i++) {
                docNames.add(dis.readUTF());
            }

            int termCount = dis.readInt();
            for (int i = 0; i < termCount; i++) {
                String term = dis.readUTF();
                int docsSize = dis.readInt();
                
                List<Integer> directDocs = new ArrayList<>(docsSize);
                List<Integer> inverseDocs = new ArrayList<>(docsSize);
                
                for (int j = 0; j < docsSize; j++) {
                    int docId = dis.readInt();
                    directDocs.add(docId);
                    inverseDocs.add(docId);
                }

                directTree.put(term, directDocs);
                
                String reversedTerm = new StringBuilder(term).reverse().toString();
                inverseTree.put(reversedTerm, inverseDocs);
            }
        }
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

    @Override
    public SearchResult getDocsForTerms(List<String> terms) {
        SearchResult combined = null;
        for (String term : terms) {
            SearchResult res = search(term);
            if (combined == null) {
                combined = res;
            } else {
                combined = combined.or(res);
            }
        }
        return combined != null ? combined : new ListResult(Collections.emptyList());
    }
}