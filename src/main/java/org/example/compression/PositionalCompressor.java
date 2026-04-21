package org.example.compression;

import java.io.*;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class PositionalCompressor {

    private static class CountingOutputStream extends FilterOutputStream {
        private long count = 0;
        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override public void write(int b) throws IOException {
            out.write(b); count++;
        }

        @Override public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len); count += len;
        }

        public long getByteOffset() {
            return count;
        }
    }

    private static void writeVBC(DataOutputStream out, int n) throws IOException {
        while (n >= 128) {
            out.writeByte((n & 0x7F) | 0x80);
            n >>>= 7;
        }
        out.writeByte(n);
    }

    public static Map<String, Long> compressPositionalIndex(Path inputPath, Path outputPath) throws IOException {
        System.out.println("Starting VBC compression on Positional Index...");
        long startTime = System.currentTimeMillis();
        
        Map<String, Long> newLexicon = new LinkedHashMap<>();

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(inputPath.toFile()), 65536));
             FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, 65536);
             CountingOutputStream cos = new CountingOutputStream(bos);
             DataOutputStream out = new DataOutputStream(cos)) {

            int totalTerms = in.readInt();
            
            for (int i = 0; i < totalTerms; i++) {
                String term = in.readUTF();
                
                newLexicon.put(term, cos.getByteOffset());
                
                int docCount = in.readInt();
                writeVBC(out, docCount);

                int prevDocId = 0;
                for (int d = 0; d < docCount; d++) {
                    int docId = in.readInt();
                    writeVBC(out, docId - prevDocId);
                    prevDocId = docId;

                    int posCount = in.readInt();
                    writeVBC(out, posCount);

                    int prevPos = 0;
                    for (int p = 0; p < posCount; p++) {
                        int pos = in.readInt();
                        writeVBC(out, pos - prevPos);
                        prevPos = pos;
                    }
                }

                if ((i + 1) % 50000 == 0) {
                    System.out.print("\rProcessed " + (i + 1) + " / " + totalTerms + " terms...");
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("\rPositional Compression finished in " + (duration / 1000) + " seconds.");
        
        return newLexicon;
    }
}