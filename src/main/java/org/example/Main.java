package org.example;

import org.example.index.*;
import org.example.parsing.DocumentParser;
import org.example.parsing.Fb2StaxParser;
import org.example.processing.LuceneSmartNormalizer;
import org.example.processing.RegexTokenizer;
import org.example.processing.TermNormalizer;
import org.example.processing.Tokenizer;
import org.example.search.BiWordSearchEngine;
import org.example.search.BooleanQueryEngine;
import org.example.search.PositionalQueryEngine;
import org.example.search.WildcardQueryEngine;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Main {
    private static final Path ROOT_DIR = Paths.get("data");
    private static final Path DOCUMENTS_DIR = ROOT_DIR.resolve("documents");
    private static final Path OUTPUT_DIR = ROOT_DIR.resolve("out");
    private static final Path INDEX_DIR = ROOT_DIR.resolve("indexes");
    private static final Path MATRIX_INDEX_FILE = INDEX_DIR.resolve("matrix.idx");
    private static final Path INVERTED_INDEX_FILE = INDEX_DIR.resolve("inverted.idx");
    private static final Path BIWORD_INDEX_FILE = INDEX_DIR.resolve("biword.idx");
    private static final Path POSITIONAL_INDEX_FILE = INDEX_DIR.resolve("positional.idx");
    private static final Path BTREE_INDEX_FILE = INDEX_DIR.resolve("btree.idx");
    //private static final Path PERMUTERM_INDEX_FILE = INDEX_DIR.resolve("permuterm.idx");
    //private static final Path KGRAM_INDEX_FILE = INDEX_DIR.resolve("kgram.idx");
    private static final Path REPORT_JSON = OUTPUT_DIR.resolve("dictionary.json");
    private static final Path REPORT_TXT = OUTPUT_DIR.resolve("dictionary.txt");
    private static final Path REPORT_BIN = OUTPUT_DIR.resolve("dictionary.bin");

    public static void main(String[] args) {
        ensureDirectories();
        practicalTask1();
        practicalTask2();
        practicalTask3BiWord();
        practicalTask3Positional();
        practicalTask4Wildcard();
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

    private static void practicalTask4Wildcard() {
        Tokenizer tokenizer = new RegexTokenizer();
        TermNormalizer normalizer = new LuceneSmartNormalizer();
        DocumentParser parser = new Fb2StaxParser();
        boolean forceRebuild = true;
        BidirectionalBTreeIndex bTreeIndex = new BidirectionalBTreeIndex(32);
        // PermutermIndex permutermIndex = new PermutermIndex();
        // KGramIndex kGramIndex = new KGramIndex(3);

        buildOrLoadSingleTermIndex(bTreeIndex, BTREE_INDEX_FILE, forceRebuild, parser, tokenizer, normalizer);
        //buildOrLoadSingleTermIndex(permutermIndex, PERMUTERM_INDEX_FILE, forceRebuild, parser, tokenizer, normalizer);
        //buildOrLoadSingleTermIndex(kGramIndex, KGRAM_INDEX_FILE, forceRebuild, parser, tokenizer, normalizer);

        List<String> wildcardQueries = List.of(
                "зах*",
                "*t",
                "lo*g",
                "*ship*",
                "g*o*al",
                "j*b*",
                "g*o*a*t"
        );

        testWildcardEngine("Bidirectional B-Tree", bTreeIndex, wildcardQueries);
        // testWildcardEngine("Permuterm Index", permutermIndex, wildcardQueries);
        // testWildcardEngine("3-Gram Index", kGramIndex, wildcardQueries);
    }

    private static void buildOrLoadSingleTermIndex(SearchIndex index, Path indexPath, boolean forceRebuild,
                                                   DocumentParser parser, Tokenizer tokenizer, TermNormalizer normalizer) {
        try {
            if (!forceRebuild && Files.exists(indexPath)) {
                System.out.println("Loading " + index.getClass().getSimpleName() + " from disk...");
                long start = System.currentTimeMillis();
                index.load(indexPath);
                System.out.println("Loaded in: " + (System.currentTimeMillis() - start) + " ms");
            } else {
                System.out.println("Building " + index.getClass().getSimpleName() + " from scratch...");
                long start = System.currentTimeMillis();

                int docIdCounter = 0;
                try (Stream<Path> fileStream = Files.list(DOCUMENTS_DIR)) {
                    List<Path> fileList = fileStream.filter(p -> p.toString().endsWith(".fb2")).toList();
                    for (Path path : fileList) {
                        int currentDocId = docIdCounter++;
                        index.registerDoc(path.getFileName().toString());

                        parser.parse(path, text -> {
                            tokenizer.tokenize(text)
                                    .map(normalizer::normalize)
                                    .filter(term -> term != null && !term.isBlank())
                                    .forEach(term -> index.add(currentDocId, term));
                        });
                    }
                }
                index.seal();
                System.out.println("Built in " + (System.currentTimeMillis() - start) + " ms. Docs: " + index.getDocCount());
                index.save(indexPath);
            }
        } catch (IOException e) {
            System.err.println("Error processing index: " + e.getMessage());
        }
    }

    private static void testWildcardEngine(String name, WildcardIndex index, List<String> queries) {
        System.out.println("\nTesting Wildcard Engine: " + name);
        WildcardQueryEngine engine = new WildcardQueryEngine(index);

        for (String q : queries) {
            long searchStart = System.nanoTime();
            List<Integer> results = engine.search(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("\nQuery: '%s' | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                int maxDisplay = 20;
                List<String> docNames = results.stream().limit(maxDisplay).map(index::getDocName).toList();
                System.out.println("Docs: " + docNames + (results.size() > maxDisplay ? " ..." : ""));
            }
        }
    }

    private static void practicalTask3Positional() {
        PositionalIndex positionalIndex = new PositionalIndex();
        Tokenizer tokenizer = new RegexTokenizer();
        TermNormalizer normalizer = new LuceneSmartNormalizer();
        DocumentParser parser = new Fb2StaxParser();

        boolean forceRebuild = true;

        try {
            if (!forceRebuild && Files.exists(POSITIONAL_INDEX_FILE)) {
                System.out.println("Loading Positional Index from disk...");
                long start = System.currentTimeMillis();
                positionalIndex.load(POSITIONAL_INDEX_FILE);
                System.out.println("Loaded in: " + (System.currentTimeMillis() - start) + " ms");
            } else {
                System.out.println("Building Positional Index from scratch...");
                long start = System.currentTimeMillis();

                int docIdCounter = 0;
                try (Stream<Path> fileStream = Files.list(DOCUMENTS_DIR)) {
                    List<Path> fileList = fileStream.filter(p -> p.toString().endsWith(".fb2")).toList();

                    for (Path path : fileList) {
                        int currentDocId = docIdCounter++;
                        positionalIndex.registerDoc(path.getFileName().toString());

                        AtomicInteger posCounter = new AtomicInteger(0);

                        parser.parse(path, text -> {
                            tokenizer.tokenize(text)
                                    .map(normalizer::normalize)
                                    .forEach(term -> {
                                        if (term != null && !term.isBlank()) {
                                            positionalIndex.add(currentDocId, term, posCounter.getAndIncrement());
                                        }
                                    });
                        });
                    }
                }

                positionalIndex.seal();
                long buildTime = System.currentTimeMillis() - start;
                System.out.println("Positional Index built in " + buildTime + " ms");
                System.out.println("Total docs: " + positionalIndex.getDocCount());

                long startSave = System.currentTimeMillis();
                positionalIndex.save(POSITIONAL_INDEX_FILE);
                System.out.println("Saved to disk in: " + (System.currentTimeMillis() - startSave) + " ms");
                System.out.println("File size: " + Files.size(POSITIONAL_INDEX_FILE) / 1024 + " KB");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        PositionalQueryEngine engine = new PositionalQueryEngine(positionalIndex, tokenizer, normalizer);

        System.out.println("\nPositional Phrase Search Test:");
        List<String> phraseQueries = List.of(
                "кинуться шукати",
                "Захар Беркут"
        );

        for (String q : phraseQueries) {
            long searchStart = System.nanoTime();
            List<Integer> results = engine.searchPhrase(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("Query: '%s' | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(positionalIndex::getDocName).toList());
            }
        }

        System.out.println("\nPositional Proximity Search Test:");
        List<String> proximityQueries = List.of(
                "ship /3 wind",
                "ship /7 wind",
                "ship /3 island",
                "Захара /5 Беркута"
        );

        for (String q : proximityQueries) {
            long searchStart = System.nanoTime();
            List<Integer> results = engine.searchProximity(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("Query: '%s' | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(positionalIndex::getDocName).toList());
            }
        }
    }

    private static void practicalTask3BiWord() {
        SearchIndex biWordIndex = new InvertedIndex();
        Tokenizer tokenizer = new RegexTokenizer();
        TermNormalizer normalizer = new LuceneSmartNormalizer();
        DocumentParser parser = new Fb2StaxParser();

        boolean forceRebuild = true;

        try {
            if (!forceRebuild && Files.exists(BIWORD_INDEX_FILE)) {
                System.out.println("Loading biword Index from disk...");
                long start = System.currentTimeMillis();
                biWordIndex.load(BIWORD_INDEX_FILE);
                System.out.println("Loaded in: " + (System.currentTimeMillis() - start) + " ms");
            } else {
                System.out.println("Building biword Index from scratch...");
                long start = System.currentTimeMillis();

                int docIdCounter = 0;

                try (Stream<Path> fileStream = Files.list(DOCUMENTS_DIR)) {
                    List<Path> fileList = fileStream.filter(p -> p.toString().endsWith(".fb2")).toList();

                    for (Path path : fileList) {
                        int currentDocId = docIdCounter++;
                        biWordIndex.registerDoc(path.getFileName().toString());

                        parser.parse(path, text -> {
                            List<String> tokens = tokenizer.tokenize(text)
                                    .map(normalizer::normalize)
                                    .filter(term -> term != null && !term.isBlank())
                                    .toList();

                            for (int i = 0; i < tokens.size() - 1; i++) {
                                String biWord = tokens.get(i) + " " + tokens.get(i + 1);
                                biWordIndex.add(currentDocId, biWord);
                            }
                        });
                    }
                }

                biWordIndex.seal();
                long buildTime = System.currentTimeMillis() - start;
                System.out.println("biword Index built in " + buildTime + " ms");
                System.out.println("Total docs: " + biWordIndex.getDocCount());

                long startSave = System.currentTimeMillis();
                biWordIndex.save(BIWORD_INDEX_FILE);
                System.out.println("Saved to disk in: " + (System.currentTimeMillis() - startSave) + " ms");
                System.out.println("File size: " + Files.size(BIWORD_INDEX_FILE) / 1024 + " KB");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        BiWordSearchEngine searchEngine = new BiWordSearchEngine(biWordIndex, tokenizer, normalizer);

        List<String> queries = List.of(
                "кинуться шукати",
                "one day",
                "Захар Беркут",
                "island of Nantucket",
                "had thought ahead of everyone else"
        );

        System.out.println("\nbiword Phrase Search Test:");
        for (String q : queries) {
            long searchStart = System.nanoTime();
            List<Integer> results = searchEngine.searchPhrase(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("Query: '%s' | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(biWordIndex::getDocName).toList());
            }
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
            List<Integer> resM = engineMatrix.search(q);
            long timeM = System.nanoTime() - startM;

            long startI = System.nanoTime();
            List<Integer> resI = engineInverted.search(q);
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

                index.registerDoc(fileName);

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