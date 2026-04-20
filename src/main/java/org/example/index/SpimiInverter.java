package org.example.index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class SpimiInverter {
    private Map<String, Map<Integer, List<Integer>>> dictionary = new HashMap<>();
    private final int tokenLimit;
    private int currentTokenCount = 0;
    private int blockCounter = 0;
    private int currentDocId = -1;
    
    private final Path outputDir;

    public SpimiInverter(int tokenLimit, Path outputDir) {
        this.tokenLimit = tokenLimit;
        this.outputDir = outputDir;
    }

    public void startNewDocument(int docId) {
        this.currentDocId = docId;
    }

    public void indexDocument(Path tokenFile) throws IOException {
        if (currentDocId < 0) {
            throw new IllegalStateException("Call startNewDocument() before indexing!");
        }

        try (Stream<String> lines = Files.lines(tokenFile)) {
            AtomicInteger positionCounter = new AtomicInteger(0);
            lines.forEach(line -> {
                String term = line.trim();
                if (!term.isEmpty()) {
                    int pos = positionCounter.getAndIncrement();
                    addToken(term, pos);
                }
            });
        }
    }

    private void addToken(String term, int position) {
        dictionary.computeIfAbsent(term, k -> new HashMap<>())
                  .computeIfAbsent(currentDocId, k -> new ArrayList<>())
                  .add(position);

        currentTokenCount++;

        if (currentTokenCount >= tokenLimit) {
            try {
                flushBlock();
            } catch (IOException e) {
                System.err.println("Failed to flush block " + blockCounter + ": " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public void flushBlock() throws IOException {
        if (dictionary.isEmpty()) return;

        System.out.println("\nFlushing block " + blockCounter + " (" + currentTokenCount + " tokens)...");

        List<String> sortedTerms = new ArrayList<>(dictionary.keySet());
        Collections.sort(sortedTerms);

        Path blockPath = outputDir.resolve("block_" + blockCounter + ".bin");

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(blockPath.toFile())))) {
            out.writeInt(sortedTerms.size());
            
            for (String term : sortedTerms) {
                out.writeUTF(term);
                
                Map<Integer, List<Integer>> postings = dictionary.get(term);
                out.writeInt(postings.size());
                
                List<Integer> sortedDocIds = new ArrayList<>(postings.keySet());
                Collections.sort(sortedDocIds);
                
                for (int docId : sortedDocIds) {
                    out.writeInt(docId);
                    
                    List<Integer> positions = postings.get(docId);
                    out.writeInt(positions.size());
                    
                    for (int pos : positions) {
                        out.writeInt(pos);
                    }
                }
            }
        }

        dictionary.clear();
        currentTokenCount = 0;
        blockCounter++;
    }
}