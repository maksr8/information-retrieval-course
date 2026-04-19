package org.example.index;

import org.example.search.BitSetResult;
import org.example.search.SearchResult;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class IncidenceMatrixIndex implements SearchIndex {
    private Map<String, BitSet> matrix = new HashMap<>();
    private final List<String> docNames = new ArrayList<>();
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
                docNames.add(in.readUTF());
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