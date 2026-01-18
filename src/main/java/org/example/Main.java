package org.example;

import org.example.parsing.DocumentParser;
import org.example.parsing.Fb2StaxParser;
import org.example.processing.RegexTokenizer;
import org.example.processing.Tokenizer;
import org.example.storage.DictionaryWriter;
import org.example.storage.JsonDictionaryWriter;
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

        try {
            Path outputPath = Paths.get("dictionary.json");
            writer.write(dictionary, outputPath);
            File file = outputPath.toFile();
            System.out.println("Dictionary saved to: " + file.getAbsolutePath());
            System.out.println("File size: " + file.length() / 1024 + " KB");
        } catch (Exception e) {
            System.err.println("Error saving dictionary: " + e.getMessage());
        }
    }
}