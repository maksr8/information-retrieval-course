package org.example.index;

import org.example.search.SearchResult;
import java.io.IOException;
import java.nio.file.Path;

public interface SearchIndex {
    void add(int docId, String term);
    SearchResult search(String term);

    void registerDoc(String docName);
    String getDocName(int docId);
    int getDocCount();

    void save(Path path) throws IOException;
    void load(Path path) throws IOException;

    void seal();
}