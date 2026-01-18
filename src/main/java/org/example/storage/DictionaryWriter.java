package org.example.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public interface DictionaryWriter {
    void write(Map<String, Integer> dictionary, Path destination) throws IOException;
}