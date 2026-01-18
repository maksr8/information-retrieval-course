package org.example.parsing;

import java.nio.file.Path;
import java.util.function.Consumer;

public interface DocumentParser {
    void parse(Path filePath, Consumer<String> textConsumer);
}