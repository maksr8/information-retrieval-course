package org.example;

import org.example.parsing.DocumentParser;
import org.example.parsing.Fb2StaxParser;
import org.example.processing.RegexTokenizer;
import org.example.processing.Tokenizer;
import org.example.storage.DictionaryWriter;
import org.example.storage.JsonDictionaryWriter;
import org.example.storage.BinaryDictionaryWriter;
import org.example.storage.TextDictionaryWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {
        Tokenizer tokenizer = new RegexTokenizer();
        DocumentParser parser = new Fb2StaxParser();
        DictionaryWriter writer = new JsonDictionaryWriter();

        Map<String, Integer> dictionary = new TreeMap<>();

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

            Path path = Paths.get("dictionary" + extension);

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
    }
}