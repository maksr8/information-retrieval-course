package org.example.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class TextDictionaryWriter implements DictionaryWriter {
    @Override
    public void write(Map<String, Integer> dictionary, Path destination) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(destination, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
                writer.write(entry.getKey());
                writer.write(" : ");
                writer.write(String.valueOf(entry.getValue()));
                writer.newLine();
            }
        }
    }
}