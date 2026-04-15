package org.example.index;

import org.example.search.SearchResult;
import org.example.search.ListResult;

import java.io.*;
import java.nio.file.Path;
import java.util.*;


public class InvertedIndex implements SearchIndex {
    private Map<String, List<Integer>> index = new HashMap<>();
    private final List<String> docNames = new ArrayList<>();
    private boolean isSealed = false;

    /**
     * Adds a term to the index for a given document ID. Document IDs must be added in increasing order.
     * @param docId
     * @param term
     */
    @Override
    public void add(int docId, String term) {
        if (isSealed) {
            throw new IllegalStateException("Index is sealed! Cannot add new documents.");
        }
        List<Integer> postings = index.computeIfAbsent(term, k -> new ArrayList<>());

        if (!postings.isEmpty()) {
            int lastId = postings.getLast();
            if (lastId == docId) {
                return;
            }
            if (lastId > docId) {
                throw new IllegalArgumentException("DocIDs must be added in increasing order! Last: " + lastId + ", New: " + docId);
            }
        }

        postings.add(docId);
    }

    /**
     * Seals the index, preventing further modifications. This method optimizes the internal data structure for search performance.
     * After sealing, no new documents can be added. The method converts the internal HashMap to a TreeMap for faster lookups and trims excess capacity from lists.
     */
    @Override
    public void seal() {
        if (isSealed) return;
        System.out.println("Sealing index: Converting HashMap to TreeMap...");

        TreeMap<String, List<Integer>> sortedIndex = new TreeMap<>(index);

        for (List<Integer> list : sortedIndex.values()) {
            ((ArrayList<Integer>) list).trimToSize(); // Reduce memory usage by trimming excess capacity
        }

        this.index = sortedIndex;
        this.isSealed = true;
    }

    @Override
    public SearchResult search(String term) {
        List<Integer> ids = index.get(term);
        return new ListResult(ids == null ? Collections.emptyList() : ids);
    }

    /**
     * Doc IDs start with 0
     * @param docName
     */
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
    public void save(Path path) throws IOException {
        if (!isSealed) {
            seal();
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeInt(docNames.size());
            for (String docName : docNames) {
                out.writeUTF(docName);
            }

            out.writeInt(index.size());
            for (Map.Entry<String, List<Integer>> entry : index.entrySet()) {
                out.writeUTF(entry.getKey());
                List<Integer> list = entry.getValue();
                out.writeInt(list.size());
                for (Integer id : list) {
                    out.writeInt(id);
                }
            }
        }
    }

    @Override
    public void load(Path path) throws IOException {
        this.index = new TreeMap<>();
        this.isSealed = true;
        docNames.clear();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int docCount = in.readInt();
            for (int i = 0; i < docCount; i++) {
                docNames.add(in.readUTF());
            }
            int termCount = in.readInt();
            for (int i = 0; i < termCount; i++) {
                String term = in.readUTF();
                int listSize = in.readInt();
                List<Integer> list = new ArrayList<>(listSize);
                for (int j = 0; j < listSize; j++) {
                    list.add(in.readInt());
                }
                index.put(term, list);
            }
        }
    }
}