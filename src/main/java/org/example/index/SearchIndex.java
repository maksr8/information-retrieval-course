package org.example.index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

public interface SearchIndex {
    void add(int docId, String term);
    Set<Integer> search(String term);

    void registerDoc(int docId, String docName);
    String getDocName(int docId);
    int getDocCount();

    void save(Path path) throws IOException;
    void load(Path path) throws IOException;

    void seal();
}