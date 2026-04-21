package org.example.index;

import org.example.search.ListResult;
import org.example.search.SearchResult;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.*;

public class SpimiDiskAdapter extends PositionalIndex implements AutoCloseable {

    private final Map<String, Long> lexicon;
    private final RandomAccessFile indexFile;
    private final boolean isCompressed;

    public SpimiDiskAdapter(Map<String, Long> lexicon, Path indexPath, boolean isCompressed) throws IOException {
        this.lexicon = lexicon;
        this.indexFile = new RandomAccessFile(indexPath.toFile(), "r");
        this.isCompressed = isCompressed;
        super.seal();
    }

    private Map<Integer, List<Integer>> fetchFromDisk(String term) {
        Long offset = lexicon.get(term);
        if (offset == null) return null;

        try {
            indexFile.seek(offset);
            if (isCompressed) {
                return readCompressedBlock();
            } else {
                return readUncompressedBlock();
            }

        } catch (IOException e) {
            System.err.println("Failed to read from disk for term: " + term + " (" + e.getMessage() + ")");
            return null;
        }
    }

    private Map<Integer, List<Integer>> readUncompressedBlock() throws IOException {
        indexFile.readUTF();

        int docCount = indexFile.readInt();
        Map<Integer, List<Integer>> postings = new LinkedHashMap<>(docCount);

        for (int i = 0; i < docCount; i++) {
            int docId = indexFile.readInt();
            int posCount = indexFile.readInt();
            List<Integer> positions = new ArrayList<>(posCount);

            for (int j = 0; j < posCount; j++) {
                positions.add(indexFile.readInt());
            }
            postings.put(docId, positions);
        }
        return postings;
    }

    private Map<Integer, List<Integer>> readCompressedBlock() throws IOException {
        int docCount = readVBC();
        Map<Integer, List<Integer>> postings = new LinkedHashMap<>(docCount);

        int currentDocId = 0;
        for (int i = 0; i < docCount; i++) {
            currentDocId += readVBC();

            int posCount = readVBC();
            List<Integer> positions = new ArrayList<>(posCount);

            int currentPos = 0;
            for (int j = 0; j < posCount; j++) {
                currentPos += readVBC();
                positions.add(currentPos);
            }
            postings.put(currentDocId, positions);
        }
        return postings;
    }

    private int readVBC() throws IOException {
        int n = 0;
        int shift = 0;
        int b;
        while (true) {
            b = indexFile.readUnsignedByte();
            n |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return n;
    }

    @Override
    public SearchResult search(String term) {
        Map<Integer, List<Integer>> docs = fetchFromDisk(term);
        return new ListResult(docs == null ? Collections.emptyList() : new ArrayList<>(docs.keySet()));
    }

    @Override
    public SearchResult searchPhrase(List<String> terms) {
        if (terms == null || terms.isEmpty())
            return new ListResult(Collections.emptyList());

        if (terms.size() == 1) {
            return search(terms.getFirst());
        }

        Map<Integer, List<Integer>> baseDocs = fetchFromDisk(terms.getFirst());
        if (baseDocs == null) return new ListResult(Collections.emptyList());

        Set<Integer> resultDocs = new HashSet<>();

        List<Map<Integer, List<Integer>>> subsequentTermDocs = new ArrayList<>();
        for (int i = 1; i < terms.size(); i++) {
            Map<Integer, List<Integer>> docs = fetchFromDisk(terms.get(i));
            if (docs == null) return new ListResult(Collections.emptyList());
            subsequentTermDocs.add(docs);
        }

        for (Map.Entry<Integer, List<Integer>> entry : baseDocs.entrySet()) {
            int docId = entry.getKey();
            List<Integer> basePositions = entry.getValue();
            boolean docMatches = false;

            for (int pos : basePositions) {
                boolean phraseMatchesAtPos = true;

                for (int i = 0; i < subsequentTermDocs.size(); i++) {
                    Map<Integer, List<Integer>> nextTermDocs = subsequentTermDocs.get(i);
                    int targetPos = pos + i + 1;

                    if (!nextTermDocs.containsKey(docId)) {
                        phraseMatchesAtPos = false;
                        break;
                    }

                    List<Integer> nextPositions = nextTermDocs.get(docId);
                    if (Collections.binarySearch(nextPositions, targetPos) < 0) {
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

        List<Integer> sortedResult = new ArrayList<>(resultDocs);
        Collections.sort(sortedResult);
        return new ListResult(sortedResult);
    }

    @Override
    public SearchResult searchProximity(String term1, String term2, int maxDistance) {
        Map<Integer, List<Integer>> docs1 = fetchFromDisk(term1);
        Map<Integer, List<Integer>> docs2 = fetchFromDisk(term2);

        if (docs1 == null || docs2 == null) return new ListResult(Collections.emptyList());

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

        List<Integer> sortedResult = new ArrayList<>(resultDocs);
        Collections.sort(sortedResult);
        return new ListResult(sortedResult);
    }

    @Override
    public void close() throws IOException {
        if (indexFile != null) {
            indexFile.close();
        }
    }

    @Override
    public void add(String term, int position) {
        throw new UnsupportedOperationException("SpimiDiskAdapter is Read-Only!");
    }

    @Override
    public void startNewDocument() {
        throw new UnsupportedOperationException("SpimiDiskAdapter is Read-Only!");
    }

    @Override
    public void save(Path path) {
        throw new UnsupportedOperationException("Index is already saved on disk by SPIMI Merger!");
    }

    @Override
    public void load(Path path) {
        throw new UnsupportedOperationException("Use the constructor to load SpimiDiskAdapter!");
    }
}