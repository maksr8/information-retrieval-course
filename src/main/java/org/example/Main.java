package org.example;

import org.example.parsing.DocumentParser;
import org.example.parsing.Fb2StaxParser;
import org.example.processing.RegexTokenizer;
import org.example.processing.Tokenizer;
import org.example.storage.DictionaryWriter;
import org.example.storage.JsonDictionaryWriter;
import org.example.storage.BinaryDictionaryWriter;
import org.example.storage.TextDictionaryWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {
        Tokenizer tokenizer = new RegexTokenizer();
        DocumentParser parser = new Fb2StaxParser();

        Collator uaCollator = Collator.getInstance(new Locale("uk", "UA"));

        Map<String, Integer> dictionary = new TreeMap<>(uaCollator);

        Path dataDir = Paths.get("data/pr1");
        if (!Files.exists(dataDir)) {
            System.err.println("Error: 'data' directory not found!");
            return;
        }

        System.out.println("Starting indexing process...");
        long startTime = System.currentTimeMillis();

        try (Stream<Path> files = Files.list(dataDir)) {
            files.filter(p -> p.toString().endsWith(".fb2"))
                    .forEach(path -> {
                        System.out.println("Processing: " + path.getFileName());
                        parser.parse(path, text -> {
                            tokenizer.tokenize(text).forEach(word ->
                                    dictionary.merge(word, 1, Integer::sum)
                            );
                        });
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Processing time: " + duration + " ms");
        System.out.println("Unique terms found: " + dictionary.size());
        long totalWords = dictionary.values().stream().mapToLong(i -> i).sum();
        System.out.println("Total words (token count): " + totalWords);

        System.out.println("\nStorage Format Comparison:");

        Path outDir = Paths.get("out");
        try {
            if (!Files.exists(outDir)) {
                Files.createDirectories(outDir);
            }
        } catch (IOException e) {
            System.err.println("Error creating 'out' directory: " + e.getMessage());
            return;
        }

        Map<String, DictionaryWriter> writers = Map.of(
                "JSON", new JsonDictionaryWriter(),
                "Plain Text", new TextDictionaryWriter(),
                "Binary", new BinaryDictionaryWriter()
        );

        System.out.printf("%-15s | %-15s | %-15s%n", "Format", "Write Time (ms)", "File Size (KB)");
        System.out.println("-------------------------------------------------------");

        for (var entry : writers.entrySet()) {
            String formatName = entry.getKey();
            DictionaryWriter currentWriter = entry.getValue();

            String extension = formatName.equals("JSON") ? ".json" :
                    formatName.equals("Plain Text") ? ".txt" : ".bin";

            Path path = outDir.resolve("dictionary" + extension);

            try {
                long startWrite = System.currentTimeMillis();
                currentWriter.write(dictionary, path);
                long writeTime = System.currentTimeMillis() - startWrite;
                long sizeKb = Files.size(path) / 1024;
                System.out.printf("%-15s | %-15d | %-15d%n", formatName, writeTime, sizeKb);
            } catch (Exception e) {
                System.err.println("Error with format " + formatName + ": " + e.getMessage());
            }
        }

        System.out.println("\nBinary file check:");
        Path binPath = outDir.resolve("dictionary.bin");

        try (java.io.DataInputStream dis = new java.io.DataInputStream(
                new java.io.BufferedInputStream(new java.io.FileInputStream(binPath.toFile())))) {

            int savedSize = dis.readInt();
            System.out.println("Word count from data: " + savedSize);

            System.out.println("First 5 entries from file:");
            for (int i = 0; i < 5; i++) {
                String word = dis.readUTF();
                int count = dis.readInt();
                System.out.println("   -> " + word + " : " + count);
            }
        } catch (Exception e) {
            System.err.println("Error reading binary file: " + e.getMessage());
        }
    }
}