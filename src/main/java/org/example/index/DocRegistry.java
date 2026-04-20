package org.example.index;

import java.io.IOException;
import java.nio.file.Path;

public interface DocRegistry {
    String getDocName(int docId);
    int getDocCount();
    void save(Path path) throws IOException;
    void load(Path path) throws IOException;
}
