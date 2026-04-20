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
import org.example.storage.BinaryDictionaryWriter;
import org.example.storage.DictionaryWriter;
import org.example.storage.JsonDictionaryWriter;
import org.example.storage.TextDictionaryWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Main {
    private static final Path ROOT_DIR = Paths.get("data");
    private static final Path DOCUMENTS_DIR = ROOT_DIR.resolve("documents");
    private static final Path OUTPUT_DIR = ROOT_DIR.resolve("out");
    private static final Path INDEX_DIR = ROOT_DIR.resolve("indexes");

    private static final Path REGISTRY_FILE = INDEX_DIR.resolve("registry.dat");
    private static final Path MATRIX_INDEX_FILE = INDEX_DIR.resolve("matrix.idx");
    private static final Path INVERTED_INDEX_FILE = INDEX_DIR.resolve("inverted.idx");
    private static final Path BIWORD_INDEX_FILE = INDEX_DIR.resolve("biword.idx");
    private static final Path POSITIONAL_INDEX_FILE = INDEX_DIR.resolve("positional.idx");

    private static final Path REPORT_JSON = OUTPUT_DIR.resolve("dictionary.json");
    private static final Path REPORT_TXT = OUTPUT_DIR.resolve("dictionary.txt");
    private static final Path REPORT_BIN = OUTPUT_DIR.resolve("dictionary.bin");

//    private static final Path GUTENBERG_TOKENS_DIR = Paths.get("D:\\1Documents\\gutenberg_full\\SPGC-tokens-2018-07-18");
    private static final Path GUTENBERG_TOKENS_DIR = Paths.get("D:\\1Documents\\gutenberg_small");
    private static final Path GUTENBERG_CSV_FILE = Paths.get("D:\\1Documents\\gutenberg_full\\SPGC-metadata-2018-07-18.csv");
    private static final Path GUTENBERG_INDEX_DIR = Paths.get("D:\\1Documents\\gutenberg_index");

    private static final Path FULL_REGISTRY_FILE = GUTENBERG_INDEX_DIR.resolve("full_registry.dat");

    private static final DocumentParser parser = new Fb2StaxParser();
    private static final Tokenizer tokenizer = new RegexTokenizer();
    private static final TermNormalizer normalizer = new LuceneSmartNormalizer();

    private static DocumentFullRegistry activeRegistry = null;

    public static void main(String[] args) {
        ensureDirectories();
//        practicalTask1();

        boolean forceRebuild = true;

//        practicalTask2(forceRebuild);
//        practicalTask3BiWord(forceRebuild);
//        practicalTask3Positional(forceRebuild);
//        practicalTask4Wildcard(forceRebuild);

        //Task 5
        buildFullRegistry();
        loadFullRegistry();
//        printFullRegistry();
        if (activeRegistry != null) {
            runSpimi();
        }
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

    private static void runSpimi() {
        int tokenLimit = 50_000_000;
        SpimiInverter inverter = new SpimiInverter(tokenLimit, GUTENBERG_INDEX_DIR);

        int totalDocs = activeRegistry.getDocCount();
        System.out.println("\nStarting SPIMI inversion for " + totalDocs + " documents...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalDocs; i++) {
            DocumentMetadata meta = activeRegistry.getMetadata(i);
            Path tokenFile = GUTENBERG_TOKENS_DIR.resolve(meta.gutenbergId() + "_tokens.txt");

            if (Files.exists(tokenFile)) {
                inverter.startNewDocument(i);
                try {
                    inverter.indexDocument(tokenFile);
                } catch (IOException e) {
                    System.err.println("\nFailed to read " + tokenFile.getFileName() + ": " + e.getMessage());
                }
            } else {
                System.err.println("\nMissing token file: " + tokenFile.getFileName());
            }

            //for every 100 docs
            if (i % 100 == 0 || i == totalDocs - 1) {
                System.out.printf("\rProcessing document %d of %d (%.2f%%)",
                        i + 1, totalDocs, (i + 1.0) / totalDocs * 100);
            }
        }

        System.out.println("\nDocument processing complete. Executing final flush...");

        try {
            inverter.flushBlock();
        } catch (IOException e) {
            System.err.println("Final flush failed: " + e.getMessage());
        }

        long duration = System.currentTimeMillis() - startTime;
        long mins = duration / 60000;
        long secs = (duration % 60000) / 1000;
        long ms = duration % 1000;
        System.out.println("SPIMI Inversion completed in " + mins + " min " + secs + " sec " + ms + " ms.");
    }

    private static void printFullRegistry() {
        if (activeRegistry == null) {
            System.out.println("No registry loaded to display.");
            return;
        }
        int totalDocs = activeRegistry.getDocCount();
        System.out.println("Full registry total documents: " + totalDocs);

        System.out.println("\nRegistry Sample");
        int displayLimit = Math.min(500, totalDocs);
        for (int i = 0; i < displayLimit; i++) {
            DocumentMetadata meta = activeRegistry.getMetadata(i);
            System.out.printf("Internal DocID: %-4d | Gutenberg ID: %-6s | Title: %s%n",
                    meta.docId(), meta.gutenbergId(), meta.title());
        }
    }

    private static void loadFullRegistry() {
        System.out.println("Loading registry from disk (" + FULL_REGISTRY_FILE + ")...");
        if (Files.exists(FULL_REGISTRY_FILE)) {
            try {
                activeRegistry = new DocumentFullRegistry();
                activeRegistry.load(FULL_REGISTRY_FILE);
            } catch (IOException e) {
                System.err.println("Error while loading the registry: " + e.getMessage());
                activeRegistry = null;
            }
        } else {
            System.err.println("Registry file not found. Please run buildFullRegistry() first.");
        }
    }

    private static void buildFullRegistry() {
        System.out.println("Reading metadata CSV from: " + GUTENBERG_CSV_FILE);
        Map<String, String[]> metadataMap = new HashMap<>();

        Pattern csvPattern = Pattern.compile("\"([^\"]*)\"|(?<=,|^)([^,]*)(?:,|$)");

        if (Files.exists(GUTENBERG_CSV_FILE)) {
            try (BufferedReader br = Files.newBufferedReader(GUTENBERG_CSV_FILE)) {
                String line;
                boolean firstLine = true;
                while ((line = br.readLine()) != null) {
                    if (firstLine) {
                        firstLine = false; continue;
                    }

                    Matcher matcher = csvPattern.matcher(line);
                    List<String> fields = new ArrayList<>();
                    while (matcher.find()) {
                        String match = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
                        fields.add(match);
                    }

                    if (fields.size() >= 3) {
                        String id = fields.get(0);
                        String title = fields.get(1);
                        String author = fields.get(2);
                        metadataMap.put(id, new String[]{title, author});
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading CSV: " + e.getMessage());
            }
        } else {
            System.err.println("WARN: CSV file not found. Metadata will be marked as 'Unknown'.");
        }

        System.out.println("Scanning token files and building registry...");
        DocumentFullRegistry registry = new DocumentFullRegistry();

        try (Stream<Path> paths = Files.list(GUTENBERG_TOKENS_DIR)) {
            paths.filter(p -> p.getFileName().toString().endsWith("_tokens.txt"))
                    .sorted((p1, p2) -> {
                        String name1 = p1.getFileName().toString().split("_")[0].replace("PG", "");
                        String name2 = p2.getFileName().toString().split("_")[0].replace("PG", "");
                        try {
                            return Integer.compare(Integer.parseInt(name1), Integer.parseInt(name2));
                        } catch (NumberFormatException ex) {
                            return name1.compareTo(name2);
                        }
                    })
                    .forEach(path -> {
                        String filename = path.getFileName().toString();
                        String gutenbergId = filename.split("_")[0];

                        String[] meta = metadataMap.getOrDefault(gutenbergId, new String[]{"Unknown Title", "Unknown Author"});
                        registry.registerDoc(gutenbergId, meta[0], meta[1]);
                    });
        } catch (IOException e) {
            System.err.println("Error reading tokens directory: " + e.getMessage());
        }

        try {
            registry.save(FULL_REGISTRY_FILE);
            System.out.println("Registry successfully saved! Total documents registered: " + registry.getDocCount());
        } catch (IOException e) {
            System.err.println("Failed to save registry: " + e.getMessage());
        }
    }

    private static void buildIndexes(DocumentRegistry registry, Map<SearchIndex, Path> activeIndexes, boolean forceRebuild) {
        if (!forceRebuild && Files.exists(REGISTRY_FILE)) {
            System.out.println("Loading registry and indexes from disk...");
            try {
                long start = System.currentTimeMillis();
                registry.load(REGISTRY_FILE);
                for (Map.Entry<SearchIndex, Path> entry : activeIndexes.entrySet()) {
                    entry.getKey().load(entry.getValue());
                }
                System.out.println("Successfully loaded in " + (System.currentTimeMillis() - start) + " ms. Total docs: " + registry.getDocCount());
                return;
            } catch (IOException e) {
                System.err.println("Failed to load from disk, rebuilding... Error: " + e.getMessage());
            }
        }

        System.out.println("Building indexes from scratch...");
        long start = System.currentTimeMillis();

        try (Stream<Path> fileStream = Files.list(DOCUMENTS_DIR)) {
            List<Path> fileList = fileStream.filter(p -> p.toString().endsWith(".fb2")).toList();

            for (Path path : fileList) {
                registry.registerDoc(path.getFileName().toString());

                for (SearchIndex index : activeIndexes.keySet()) {
                    index.startNewDocument();
                }

                AtomicInteger posCounter = new AtomicInteger(0);
                parser.parse(path, text -> {
                    tokenizer.tokenize(text)
                            .map(normalizer::normalize)
                            .filter(term -> term != null && !term.isBlank())
                            .forEach(term -> {
                                int pos = posCounter.getAndIncrement();
                                for (SearchIndex index : activeIndexes.keySet()) {
                                    index.add(term, pos);
                                }
                            });
                });
            }
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
        }

        System.out.println("Sealing and saving to disk...");
        try {
            registry.save(REGISTRY_FILE);
            for (Map.Entry<SearchIndex, Path> entry : activeIndexes.entrySet()) {
                SearchIndex index = entry.getKey();
                index.seal();
                index.save(entry.getValue());
            }
            long time = System.currentTimeMillis() - start;
            System.out.println("Built and saved in " + time + " ms. Docs: " + registry.getDocCount());
        } catch (IOException e) {
            System.err.println("Error saving to disk: " + e.getMessage());
        }
    }

    private static void practicalTask4Wildcard(boolean forceRebuild) {
        System.out.println("\n                  Task 4");
        DocumentRegistry registry = new DocumentRegistry();
        InvertedIndex invertedIndex = new InvertedIndex();

        buildIndexes(registry, Map.of(invertedIndex, INVERTED_INDEX_FILE), forceRebuild);

        List<String> dictionary = invertedIndex.getAllTerms();

        System.out.println("\nBuilding wildcard dictionaries from " + dictionary.size() + " unique terms...");
        long startWildcard = System.currentTimeMillis();
        BidirectionalBTreeIndex bTreeIndex = new BidirectionalBTreeIndex(32);
        PermutermIndex permutermIndex = new PermutermIndex(32);
        KGramIndex kGramIndex = new KGramIndex(3, 32);
        bTreeIndex.buildFromDictionary(dictionary);
        permutermIndex.buildFromDictionary(dictionary);
        kGramIndex.buildFromDictionary(dictionary);
        System.out.println("Wildcard dictionaries built in " + (System.currentTimeMillis() - startWildcard) + " ms.");

        List<String> wildcardQueries = List.of(
                "зах*",
                "*t",
                "lo*g",
                "*ship*",
                "g*o*al",
                "j*b*",
                "g*o*a*t",
                "*iq*"
        );

        testWildcardEngine("Bidirectional B-Tree", new WildcardQueryEngine(bTreeIndex, invertedIndex), wildcardQueries, registry);
        testWildcardEngine("Permuterm Index", new WildcardQueryEngine(permutermIndex, invertedIndex), wildcardQueries, registry);
        testWildcardEngine("k-Gram Index", new WildcardQueryEngine(kGramIndex, invertedIndex), wildcardQueries, registry);
    }

    private static void testWildcardEngine(String name, WildcardQueryEngine engine, List<String> queries, DocumentRegistry registry) {
        System.out.println("\n             Testing Wildcard Engine: " + name);

        for (String q : queries) {
            long searchStart = System.nanoTime();
            List<Integer> results = engine.search(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("\nQuery: \"%s\" | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                int maxDisplay = 20;
                List<String> docNames = results.stream().limit(maxDisplay).map(registry::getDocName).toList();
                System.out.println("Docs: " + docNames + (results.size() > maxDisplay ? " ..." : ""));
            }
        }
    }

    private static void practicalTask3Positional(boolean forceRebuild) {
        System.out.println("\n                 Task 3: Positional Index");
        DocumentRegistry registry = new DocumentRegistry();
        PositionalIndex positionalIndex = new PositionalIndex();

        buildIndexes(registry, Map.of(positionalIndex, POSITIONAL_INDEX_FILE), forceRebuild);

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

            System.out.printf("Query: \"%s\" | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(registry::getDocName).toList());
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

            System.out.printf("Query: \"%s\" | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(registry::getDocName).toList());
            }
        }
    }

    private static void practicalTask3BiWord(boolean forceRebuild) {
        System.out.println("\n                  Task 3: BiWord Index");
        DocumentRegistry registry = new DocumentRegistry();

        SearchIndex biWordIndex = new BiWordIndex(new InvertedIndex());

        buildIndexes(registry, Map.of(biWordIndex, BIWORD_INDEX_FILE), forceRebuild);

        BiWordSearchEngine searchEngine = new BiWordSearchEngine(biWordIndex, tokenizer, normalizer);

        List<String> queries = List.of(
                "кинуться шукати",
                "one day",
                "Захар Беркут",
                "island of Nantucket",
                "had thought ahead of everyone else"
        );

        System.out.println("\nBiWord Phrase Search Test:");
        for (String q : queries) {
            long searchStart = System.nanoTime();
            List<Integer> results = searchEngine.searchPhrase(q);
            long searchTime = System.nanoTime() - searchStart;

            System.out.printf("Query: \"%s\" | Found: %d docs (%,d ns)\n", q, results.size(), searchTime);
            if (!results.isEmpty()) {
                System.out.println("Docs: " + results.stream().map(registry::getDocName).toList());
            }
        }
    }

    private static void practicalTask2(boolean forceRebuild) {
        System.out.println("\n                 Task 2");
        DocumentRegistry registry = new DocumentRegistry();
        SearchIndex matrixIndex = new IncidenceMatrixIndex();
        SearchIndex invertedIndex = new InvertedIndex();

        Map<SearchIndex, Path> indexes = Map.of(
                matrixIndex, MATRIX_INDEX_FILE,
                invertedIndex, INVERTED_INDEX_FILE
        );

        buildIndexes(registry, indexes, forceRebuild);

        BooleanQueryEngine engineMatrix = new BooleanQueryEngine(matrixIndex, normalizer, registry.getDocCount());
        BooleanQueryEngine engineInverted = new BooleanQueryEngine(invertedIndex, normalizer, registry.getDocCount());

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
            System.out.println("\nQuery: \"" + q + '"');

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
                        .map(registry::getDocName).toList();
                System.out.println("Docs: " + docNames);
            }
        }
    }

    private static void practicalTask1() {
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