package org.example.index;

import org.example.search.SearchResult;

import java.io.IOException;
import java.nio.file.Path;

public interface SearchIndex {
    void add(String term, int position);
    void startNewDocument();

    SearchResult search(String term);

    void save(Path path) throws IOException;
    void load(Path path) throws IOException;

    void seal();
}