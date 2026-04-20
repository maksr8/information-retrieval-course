package org.example.index;

import org.example.search.BitSetResult;
import org.example.search.SearchResult;

import java.io.*;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class IncidenceMatrixIndex implements SearchIndex {
    private Map<String, BitSet> matrix = new HashMap<>();
    private int currentDocId = -1;
    private boolean isSealed = false;

    @Override
    public void add(String term, int position) {
        if (isSealed) throw new IllegalStateException("Index is sealed!");
        if (currentDocId < 0) throw new IllegalStateException("Call startNewDocument() first!");

        matrix.computeIfAbsent(term, k -> new BitSet()).set(currentDocId);
    }

    @Override
    public void startNewDocument() {
        if (isSealed) throw new IllegalStateException("Index is sealed!");
        currentDocId++;
    }

    @Override
    public SearchResult search(String term) {
        if (!isSealed) throw new IllegalStateException("Index is not sealed!");
        BitSet bits = matrix.get(term);
        return new BitSetResult(bits == null ? new BitSet() : bits);
    }

    @Override
    public void save(Path path) throws IOException {
        if (!isSealed) {
            seal();
        }
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeInt(matrix.size());
            for (Map.Entry<String, BitSet> entry : matrix.entrySet()) {
                out.writeUTF(entry.getKey());

                long[] longs = entry.getValue().toLongArray();
                out.writeInt(longs.length);
                for (long l : longs) {
                    out.writeLong(l);
                }
            }
        }
    }

    @Override
    public void load(Path path) throws IOException {
        this.matrix = new TreeMap<>();
        this.isSealed = true;

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int termsCount = in.readInt();
            for (int i = 0; i < termsCount; i++) {
                String term = in.readUTF();

                int longArrayLength = in.readInt();
                long[] longs = new long[longArrayLength];
                for (int j = 0; j < longArrayLength; j++) {
                    longs[j] = in.readLong();
                }

                matrix.put(term, BitSet.valueOf(longs));
            }
        }
    }

    @Override
    public void seal() {
        if (isSealed) return;
        System.out.println("Sealing Incidence Matrix Index: Converting HashMap to TreeMap...");

        this.matrix = new TreeMap<>(matrix);
        this.isSealed = true;
    }
}