package org.example.index;

import org.example.search.BitSetResult;
import org.example.search.SearchResult;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class IncidenceMatrixIndex implements SearchIndex {
    private Map<String, BitSet> matrix = new HashMap<>();
    private final Map<Integer, String> docNames = new HashMap<>();
    private boolean isSealed = false;

    @Override
    public void add(int docId, String term) {
        matrix.computeIfAbsent(term, k -> new BitSet()).set(docId);
    }

    @Override
    public SearchResult search(String term) {
        BitSet bits = matrix.get(term);
        return new BitSetResult(bits == null ? new BitSet() : bits);
    }

    @Override
    public void registerDoc(int docId, String docName) {
        docNames.put(docId, docName);
    }

    @Override
    public String getDocName(int docId) {
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
            for (Map.Entry<Integer, String> entry : docNames.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeUTF(entry.getValue());
            }

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
        docNames.clear();

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int docsCount = in.readInt();
            for (int i = 0; i < docsCount; i++) {
                int id = in.readInt();
                String name = in.readUTF();
                docNames.put(id, name);
            }

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