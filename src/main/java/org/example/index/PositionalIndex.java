package org.example.index;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class PositionalIndex {
    private Map<String, Map<Integer, List<Integer>>> index = new HashMap<>();
    private final Map<Integer, String> docNames = new HashMap<>();
    private boolean isSealed = false;

    public void add(int docId, String term, int position) {
        if (isSealed) throw new IllegalStateException("Index is sealed!");
        
        index.computeIfAbsent(term, k -> new HashMap<>())
             .computeIfAbsent(docId, k -> new ArrayList<>())
             .add(position);
    }

    public Set<Integer> searchPhrase(List<String> terms) {
        if (terms == null || terms.isEmpty()) return Collections.emptySet();

        if (terms.size() == 1) {
            Map<Integer, List<Integer>> docs = index.get(terms.getFirst());
            return docs == null ? Collections.emptySet() : new HashSet<>(docs.keySet());
        }

        Map<Integer, List<Integer>> baseDocs = index.get(terms.getFirst());
        if (baseDocs == null) return Collections.emptySet();

        Set<Integer> resultDocs = new HashSet<>();

        for (Map.Entry<Integer, List<Integer>> entry : baseDocs.entrySet()) {
            int docId = entry.getKey();
            List<Integer> basePositions = entry.getValue();
            boolean docMatches = false;

            for (int pos : basePositions) {
                boolean phraseMatchesAtPos = true;

                for (int i = 1; i < terms.size(); i++) {
                    Map<Integer, List<Integer>> nextTermDocs = index.get(terms.get(i));
                    if (nextTermDocs == null || 
                        !nextTermDocs.containsKey(docId) || 
                        !nextTermDocs.get(docId).contains(pos + i)) { 
                        phraseMatchesAtPos = false;
                        break;
                    }
                }

                if (phraseMatchesAtPos) {
                    docMatches = true;
                    break;
                }
            }

            if (docMatches) resultDocs.add(docId);
        }
        return resultDocs;
    }

    public Set<Integer> searchProximity(String term1, String term2, int maxDistance) {
        Map<Integer, List<Integer>> docs1 = index.get(term1);
        Map<Integer, List<Integer>> docs2 = index.get(term2);

        if (docs1 == null || docs2 == null) return Collections.emptySet();

        Set<Integer> commonDocs = new HashSet<>(docs1.keySet());
        commonDocs.retainAll(docs2.keySet());

        Set<Integer> resultDocs = new HashSet<>();
        
        for (int docId : commonDocs) {
            List<Integer> pos1 = docs1.get(docId);
            List<Integer> pos2 = docs2.get(docId);

            int i = 0, j = 0;
            boolean found = false;
            while (i < pos1.size() && j < pos2.size()) {
                int p1 = pos1.get(i);
                int p2 = pos2.get(j);

                if (Math.abs(p1 - p2) <= maxDistance) {
                    found = true;
                    break; 
                } else if (p1 < p2) {
                    i++;
                } else {
                    j++;
                }
            }

            if (found) resultDocs.add(docId);
        }
        return resultDocs;
    }

    public void registerDoc(int docId, String docName) {
        docNames.put(docId, docName);
    }

    public String getDocName(int docId) {
        return docNames.get(docId);
    }

    public int getDocCount() {
        return docNames.size();
    }

    public void seal() {
        if (isSealed) return;
        System.out.println("Sealing Positional Index...");
        TreeMap<String, Map<Integer, List<Integer>>> sortedIndex = new TreeMap<>();
        for (Map.Entry<String, Map<Integer, List<Integer>>> entry : index.entrySet()) {
            TreeMap<Integer, List<Integer>> sortedDocs = new TreeMap<>(entry.getValue());
            for (List<Integer> positions : sortedDocs.values()) {
                ((ArrayList<Integer>) positions).trimToSize();
            }
            sortedIndex.put(entry.getKey(), sortedDocs);
        }
        this.index = sortedIndex;
        this.isSealed = true;
    }

    public void save(Path path) throws IOException {
        if (!isSealed) seal();
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeInt(docNames.size());
            for (Map.Entry<Integer, String> entry : docNames.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            out.writeInt(index.size());
            for (Map.Entry<String, Map<Integer, List<Integer>>> entry : index.entrySet()) {
                out.writeUTF(entry.getKey());
                Map<Integer, List<Integer>> docs = entry.getValue();
                out.writeInt(docs.size());
                for (Map.Entry<Integer, List<Integer>> docEntry : docs.entrySet()) {
                    out.writeInt(docEntry.getKey());
                    List<Integer> posList = docEntry.getValue();
                    out.writeInt(posList.size());
                    for (int p : posList) {
                        out.writeInt(p);
                    }
                }
            }
        }
    }

    public void load(Path path) throws IOException {
        this.index = new TreeMap<>();
        this.isSealed = true;
        docNames.clear();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int docCount = in.readInt();
            for (int i = 0; i < docCount; i++) {
                docNames.put(in.readInt(), in.readUTF());
            }
            int termCount = in.readInt();
            for (int i = 0; i < termCount; i++) {
                String term = in.readUTF();
                int docMapSize = in.readInt();
                Map<Integer, List<Integer>> docs = new TreeMap<>();
                for (int j = 0; j < docMapSize; j++) {
                    int docId = in.readInt();
                    int posCount = in.readInt();
                    List<Integer> posList = new ArrayList<>(posCount);
                    for (int k = 0; k < posCount; k++) {
                        posList.add(in.readInt());
                    }
                    docs.put(docId, posList);
                }
                index.put(term, docs);
            }
        }
    }
}