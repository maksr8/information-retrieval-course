package org.example.index;

import org.example.search.ListResult;
import org.example.search.SearchResult;

import java.io.*;
import java.nio.file.Path;
import java.util.*;


public class InvertedIndex implements SearchIndex {
    private Map<String, List<Integer>> index = new HashMap<>();
    private int currentDocId = -1;
    private boolean isSealed = false;

    /**
     * Adds a term to the index for the current document. Position is ignored
     * @param term
     * @param position
     */
    @Override
    public void add(String term, int position) {
        if (isSealed) throw new IllegalStateException("Index is sealed!");
        if (currentDocId < 0) throw new IllegalStateException("Call startNewDocument() first!");

        List<Integer> postings = index.computeIfAbsent(term, k -> new ArrayList<>());

        if (!postings.isEmpty()) {
            int lastId = postings.getLast();
            if (lastId == currentDocId) {
                return;
            }
        }
        postings.add(currentDocId);
    }

    @Override
    public void startNewDocument() {
        if (isSealed) throw new IllegalStateException("Index is sealed!");
        currentDocId++;
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
        if (!isSealed) throw new IllegalStateException("Index is not sealed!");
        List<Integer> ids = index.get(term);
        return new ListResult(ids == null ? Collections.emptyList() : ids);
    }

    @Override
    public void save(Path path) throws IOException {
        if (!isSealed) {
            seal();
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
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
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
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

    public List<String> getAllTerms() {
        return new ArrayList<>(index.keySet());
    }
}