package org.example;

import org.example.index.IncidenceMatrixIndex;
import org.example.index.InvertedIndex;
import org.example.index.SearchIndex;
import org.example.parsing.DocumentParser;
import org.example.parsing.Fb2StaxParser;
import org.example.processing.LuceneSmartNormalizer;
import org.example.processing.RegexTokenizer;
import org.example.processing.TermNormalizer;
import org.example.processing.Tokenizer;
import org.example.search.BooleanQueryEngine;
import org.example.storage.DictionaryWriter;
import org.example.storage.JsonDictionaryWriter;
import org.example.storage.BinaryDictionaryWriter;
import org.example.storage.TextDictionaryWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.*;
import java.util.stream.Stream;

public class Main {
    private static final Path ROOT_DIR = Paths.get("data");
    private static final Path DOCUMENTS_DIR = ROOT_DIR.resolve("documents");
    private static final Path OUTPUT_DIR = ROOT_DIR.resolve("out");
    private static final Path INDEX_DIR = ROOT_DIR.resolve("indexes");
    private static final Path MATRIX_INDEX_FILE = INDEX_DIR.resolve("matrix.idx");
    private static final Path INVERTED_INDEX_FILE = INDEX_DIR.resolve("inverted.idx");
    private static final Path REPORT_JSON = OUTPUT_DIR.resolve("dictionary.json");
    private static final Path REPORT_TXT = OUTPUT_DIR.resolve("dictionary.txt");
    private static final Path REPORT_BIN = OUTPUT_DIR.resolve("dictionary.bin");

    public static void main(String[] args) {
        ensureDirectories();
//        practicalTask1();
        practicalTask2();
    }

    private static void ensureDirectories() {
        try {
            Files.createDirectories(DOCUMENTS_DIR);
            Files.createDirectories(OUTPUT_DIR);
            Files.createDirectories(INDEX_DIR);
        } catch (IOException e) {
            System.err.println("Error: Cannot create directories. " + e.getMessage());
            System.exit(1);
        }
    }

    private static void practicalTask2() {
        SearchIndex matrixIndex = new IncidenceMatrixIndex();
        SearchIndex invertedIndex = new InvertedIndex();

        boolean forceRebuild = true;

        try {
            System.out.println("Processing incidence matrix index...");
            processIndex(matrixIndex, MATRIX_INDEX_FILE, DOCUMENTS_DIR, forceRebuild);
            System.out.println("\nProcessing inverted index...");
            processIndex(invertedIndex, INVERTED_INDEX_FILE, DOCUMENTS_DIR, forceRebuild);
        } catch (Exception e) {
            e.printStackTrace();
        }
        runSearchEngine(matrixIndex, invertedIndex);
    }

    private static void runSearchEngine(SearchIndex matrix, SearchIndex inverted) {
        TermNormalizer normalizer = new LuceneSmartNormalizer();
        BooleanQueryEngine engineMatrix = new BooleanQueryEngine(matrix, normalizer);
        BooleanQueryEngine engineInverted = new BooleanQueryEngine(inverted, normalizer);

        List<String> queries = List.of(
                "friend",
                "not",
                "friend AND swiftness",
                "swiftness OR Hillingham",
                "Hillingham AND NOT swiftness",
                "(Hillingham OR swiftness) AND Morrel",
                "NOT (Hillingham OR swiftness)",
                "Захар"
        );

        System.out.println("\nBoolean search test:");

        for (String q : queries) {
            System.out.println("\nQuery: " + q);

            long startM = System.nanoTime();
            Set<Integer> resM = engineMatrix.search(q);
            long timeM = System.nanoTime() - startM;

            long startI = System.nanoTime();
            Set<Integer> resI = engineInverted.search(q);
            long timeI = System.nanoTime() - startI;

            System.out.printf("Matrix:   %d hits (%,d ns)\n", resM.size(), timeM);
            System.out.printf("Inverted: %d hits (%,d ns)\n", resI.size(), timeI);

            if (!resM.equals(resI)) {
                System.err.println("Results differ!");
            } else {
                List<String> docNames = resM.stream() // .limit(5)
                        .map(matrix::getDocName).toList();
                System.out.println("Docs: " + docNames);
            }
        }
    }

    private static void processIndex(SearchIndex index, Path storagePath, Path dataDir, boolean rebuild) throws IOException {
        if (!rebuild && Files.exists(storagePath)) {
            long start = System.currentTimeMillis();
            index.load(storagePath);
            System.out.println("Loaded from disk in: " + (System.currentTimeMillis() - start) + " ms");
        } else {
            long buildTime = buildIndex(index, dataDir);
            System.out.println("Built from scratch in: " + buildTime + " ms");

            long startSave = System.currentTimeMillis();
            index.save(storagePath);
            System.out.println("Saved to disk in: " + (System.currentTimeMillis() - startSave) + " ms");
            System.out.println("File size: " + Files.size(storagePath) / 1024 + " KB");
        }
    }

    private static long buildIndex(SearchIndex index, Path dataDir) throws IOException {
        long start = System.currentTimeMillis();

        DocumentParser parser = new Fb2StaxParser();
        Tokenizer tokenizer = new RegexTokenizer();
        TermNormalizer normalizer = new LuceneSmartNormalizer();

        int docIdCounter = 0;

        try (Stream<Path> files = Files.list(dataDir)) {
            List<Path> fileList = files.filter(p -> p.toString().endsWith(".fb2")).toList();

            for (Path path : fileList) {
                int currentDocId = docIdCounter++;
                String fileName = path.getFileName().toString();

                index.registerDoc(currentDocId, fileName);

                parser.parse(path, text -> {
                    tokenizer.tokenize(text)
                            .map(normalizer::normalize)
                            .filter(term -> term != null && !term.isBlank())
                            .forEach(term -> {
                                index.add(currentDocId, term);
                            });
                });
            }
        }
        index.seal();
        return System.currentTimeMillis() - start;
    }


    private static void practicalTask1() {
        Tokenizer tokenizer = new RegexTokenizer();
        DocumentParser parser = new Fb2StaxParser();

        Collator uaCollator = Collator.getInstance(Locale.forLanguageTag("uk-UA"));
        Map<String, Integer> sortedDictionary = new TreeMap<>(uaCollator);

        Map<String, Integer> dictionary = new HashMap<>();

        System.out.println("Starting building dictionary...");
        long startTime = System.currentTimeMillis();

        try (Stream<Path> files = Files.list(DOCUMENTS_DIR)) {
            files.filter(p -> p.toString().endsWith(".fb2"))
                    .forEach(path -> {
                        System.out.println("Processing: " + path.getFileName());
                        parser.parse(path, text -> {
                            tokenizer.tokenize(text)
                                    .map(String::toLowerCase)
                                    .forEach(word ->
                                    dictionary.merge(word, 1, Integer::sum)
                            );
                        });
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Sorting dictionary...");
        sortedDictionary.putAll(dictionary);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Processing time: " + duration + " ms");
        System.out.println("Unique terms found: " + sortedDictionary.size());
        long totalWords = sortedDictionary.values().stream().mapToLong(i -> i).sum();
        System.out.println("Total words (token count): " + totalWords);

        System.out.println("\nStorage Format Comparison:");

        record WriterTask(String name, DictionaryWriter writer, Path path) {}

        List<WriterTask> tasks = List.of(
                new WriterTask("JSON", new JsonDictionaryWriter(), REPORT_JSON),
                new WriterTask("Plain Text", new TextDictionaryWriter(), REPORT_TXT),
                new WriterTask("Binary", new BinaryDictionaryWriter(), REPORT_BIN)
        );

        System.out.printf("%-15s | %-15s | %-15s%n", "Format", "Write Time (ms)", "File Size (KB)");
        System.out.println("-------------------------------------------------------");

        for (var task : tasks) {
            try {
                long startWrite = System.currentTimeMillis();
                task.writer.write(sortedDictionary, task.path);
                long writeTime = System.currentTimeMillis() - startWrite;
                long sizeKb = Files.size(task.path) / 1024;
                System.out.printf("%-15s | %-15d | %-15d%n", task.name, writeTime, sizeKb);
            } catch (Exception e) {
                System.err.println("Error " + task.name + ": " + e.getMessage());
            }
        }

        checkBinaryFile(REPORT_BIN);
    }

    private static void checkBinaryFile(Path path) {
        System.out.println("\nBinary file check (" + path.getFileName() + "):");
        try (var dis = new java.io.DataInputStream(
                new java.io.BufferedInputStream(new java.io.FileInputStream(path.toFile())))) {

            int count = dis.readInt();
            System.out.println("Word count: " + count);
            System.out.println("First 5 entries:");
            for (int i = 0; i < Math.min(count, 5); i++) {
                System.out.println("   -> " + dis.readUTF() + " : " + dis.readInt());
            }
        } catch (Exception e) {
            System.err.println("Read error: " + e.getMessage());
        }
    }
}