package org.example.index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class SpimiMerger {

    private static class CountingOutputStream extends FilterOutputStream {
        private long count = 0;

        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }

        public long getByteOffset() {
            return count;
        }
    }

    private static class BlockStream implements Comparable<BlockStream>, Closeable {
        private final DataInputStream in;
        private int termsLeft;
        public final int blockId;

        public String currentTerm;
        public int currentDocCount;

        public BlockStream(Path blockPath) throws IOException {
            String fileName = blockPath.getFileName().toString();
            this.blockId = Integer.parseInt(fileName.replaceAll("\\D+", ""));

            this.in = new DataInputStream(new BufferedInputStream(new FileInputStream(blockPath.toFile()), 65536));
            this.termsLeft = in.readInt();
            readNextTerm();
        }

        public void readNextTerm() throws IOException {
            if (termsLeft == 0) {
                currentTerm = null;
                return;
            }
            currentTerm = in.readUTF();
            currentDocCount = in.readInt();
            termsLeft--;
        }

        public void streamPostingsTo(DataOutputStream out) throws IOException {
            for (int i = 0; i < currentDocCount; i++) {
                int docId = in.readInt();
                out.writeInt(docId);

                int posCount = in.readInt();
                out.writeInt(posCount);

                for (int j = 0; j < posCount; j++) {
                    out.writeInt(in.readInt());
                }
            }
        }

        @Override
        public int compareTo(BlockStream other) {
            return this.currentTerm.compareTo(other.currentTerm);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    public static Map<String, Long> mergeBlocks(Path indexDir, Path outputIndexPath) throws IOException {
        PriorityQueue<BlockStream> minHeap = new PriorityQueue<>();
        List<Path> existingBlocksPaths = new ArrayList<>();

        try (Stream<Path> paths = Files.list(indexDir)) {
            paths.filter(p -> p.getFileName().toString().matches("block_\\d+\\.bin"))
                    .forEach(existingBlocksPaths::add);
        }

        if (existingBlocksPaths.isEmpty()) {
            System.out.println("No block files found in " + indexDir);
            return Collections.emptyMap();
        }

        System.out.println("\nFound " + existingBlocksPaths.size() + " blocks. Starting the merge...");

        for (Path blockPath : existingBlocksPaths) {
            minHeap.add(new BlockStream(blockPath));
        }

        Map<String, Long> lexicon = new LinkedHashMap<>();
        int totalUniqueTerms = 0;

        FileOutputStream fos = new FileOutputStream(outputIndexPath.toFile());
        BufferedOutputStream bos = new BufferedOutputStream(fos, 65536);
        CountingOutputStream cos = new CountingOutputStream(bos);

        try (DataOutputStream out = new DataOutputStream(cos)) {
            out.writeInt(0);

            while (!minHeap.isEmpty()) {
                BlockStream minStream = minHeap.poll();
                String term = minStream.currentTerm;

                List<BlockStream> streamsWithTerm = new ArrayList<>();
                streamsWithTerm.add(minStream);

                while (!minHeap.isEmpty() && minHeap.peek().currentTerm.equals(term)) {
                    streamsWithTerm.add(minHeap.poll());
                }

                streamsWithTerm.sort(Comparator.comparingInt(bs -> bs.blockId));

                long currentOffset = cos.getByteOffset();
                lexicon.put(term, currentOffset);

                int totalDocCount = 0;
                for (BlockStream bs : streamsWithTerm) {
                    totalDocCount += bs.currentDocCount;
                }

                out.writeUTF(term);
                out.writeInt(totalDocCount);

                for (BlockStream bs : streamsWithTerm) {
                    bs.streamPostingsTo(out);
                }

                totalUniqueTerms++;

                if (totalUniqueTerms % 5000 == 0) {
                    System.out.print("\rProcessed " + totalUniqueTerms + " unique terms...");
                    out.flush();
                }

                for (BlockStream bs : streamsWithTerm) {
                    bs.readNextTerm();
                    if (bs.currentTerm != null) {
                        minHeap.add(bs);
                    } else {
                        bs.close();
                    }
                }
            }
        }

        try (RandomAccessFile raf = new RandomAccessFile(outputIndexPath.toFile(), "rw")) {
            raf.seek(0);
            raf.writeInt(totalUniqueTerms);
        }

        System.out.println("\rMerging complete! Total unique terms: " + totalUniqueTerms + "          ");

        // System.out.println("Cleaning up temporary blocks...");
        // for (Path blockPath : existingBlocksPaths) {
        //     Files.deleteIfExists(blockPath);
        // }

        return lexicon;
    }

    public static void saveLexicon(Map<String, Long> lexicon, Path lexiconPath) {
        System.out.println("Saving Lexicon to disk...");
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(lexiconPath.toFile()), 65536))) {

            out.writeInt(lexicon.size());
            for (Map.Entry<String, Long> entry : lexicon.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeLong(entry.getValue());
            }
        } catch (IOException e) {
            System.err.println("Error saving lexicon: " + e.getMessage());
        }
        System.out.println("Lexicon successfully saved to " + lexiconPath.getFileName());
    }

    public static Map<String, Long> loadLexicon(Path lexiconPath) {
        System.out.println("Loading Lexicon from disk...");
        Map<String, Long> lexicon = new LinkedHashMap<>();

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(lexiconPath.toFile()), 65536))) {

            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                lexicon.put(in.readUTF(), in.readLong());
            }
        } catch (IOException e) {
            System.err.println("Error loading lexicon: " + e.getMessage());
        }
        System.out.println("Lexicon loaded successfully! (" + lexicon.size() + " terms)");
        return lexicon;
    }
}