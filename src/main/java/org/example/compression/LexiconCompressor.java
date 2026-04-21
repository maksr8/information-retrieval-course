package org.example.compression;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

public class LexiconCompressor {

    private static final int BLOCK_SIZE = 8;

    private static int getCommonPrefixLength(String s1, String s2) {
        int minLength = Math.min(s1.length(), s2.length());
        for (int i = 0; i < minLength; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return i;
            }
        }
        return minLength;
    }

    public static void compressFrontCoding(Map<String, Long> lexicon, Path outputPath) throws IOException {
        System.out.println("Compressing using front coding...");
        long startTime = System.currentTimeMillis();

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputPath.toFile()), 65536))) {

            out.writeInt(lexicon.size());

            List<Map.Entry<String, Long>> block = new ArrayList<>(BLOCK_SIZE);
            for (Map.Entry<String, Long> entry : lexicon.entrySet()) {
                block.add(entry);
                if (block.size() == BLOCK_SIZE) {
                    writeBlock(out, block);
                    block.clear();
                }
            }
            if (!block.isEmpty()) {
                writeBlock(out, block);
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Compression finished in " + duration + " ms.");
    }

    private static void writeBlock(DataOutputStream out, List<Map.Entry<String, Long>> block) throws IOException {
        out.writeByte(block.size());

        String commonPrefix = block.getFirst().getKey();
        for (int i = 1; i < block.size(); i++) {
            int len = getCommonPrefixLength(commonPrefix, block.get(i).getKey());
            commonPrefix = commonPrefix.substring(0, len);
        }

        byte[] prefixBytes = commonPrefix.getBytes(StandardCharsets.UTF_8);

        out.writeByte(prefixBytes.length);
        out.write(prefixBytes);

        for (Map.Entry<String, Long> entry : block) {
            String suffix = entry.getKey().substring(commonPrefix.length());
            byte[] suffixBytes = suffix.getBytes(StandardCharsets.UTF_8);

            out.writeByte(suffixBytes.length);
            out.write(suffixBytes);
            out.writeLong(entry.getValue());
        }
    }

    public static Map<String, Long> decompressFrontCoding(Path inputPath) throws IOException {
        System.out.println("Decompressing Front Coding...");
        long startTime = System.currentTimeMillis();

        Map<String, Long> decompressedLexicon = new LinkedHashMap<>();

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(inputPath.toFile()), 65536))) {

            int totalTerms = in.readInt();
            int termsRead = 0;

            while (termsRead < totalTerms) {
                int termsInBlock = in.readUnsignedByte();

                int prefixLen = in.readUnsignedByte();
                byte[] prefixBytes = new byte[prefixLen];
                in.readFully(prefixBytes);
                String prefix = new String(prefixBytes, StandardCharsets.UTF_8);

                for (int i = 0; i < termsInBlock; i++) {
                    int suffixLen = in.readUnsignedByte();
                    byte[] suffixBytes = new byte[suffixLen];
                    in.readFully(suffixBytes);
                    String suffix = new String(suffixBytes, StandardCharsets.UTF_8);

                    long offset = in.readLong();

                    decompressedLexicon.put(prefix + suffix, offset);
                    termsRead++;
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Decompression finished in " + duration + " ms.");

        return decompressedLexicon;
    }
}