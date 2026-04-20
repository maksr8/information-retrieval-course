package org.example.index;

import org.example.search.SearchResult;

import java.io.IOException;
import java.nio.file.Path;

public class BiWordIndex implements SearchIndex {
    private final SearchIndex internalIndex; 
    private String previousTerm = null;

    public BiWordIndex(SearchIndex internalIndex) {
        this.internalIndex = internalIndex;
    }

    @Override
    public void startNewDocument() {
        this.previousTerm = null;
        internalIndex.startNewDocument();
    }

    @Override
    public void add(String term, int position) {
        if (previousTerm != null) {
            String biWord = previousTerm + " " + term;
            internalIndex.add(biWord, position); 
        }
        this.previousTerm = term;
    }

    @Override
    public SearchResult search(String term) {
        return internalIndex.search(term);
    }

    @Override
    public void seal() {
        internalIndex.seal();
    }

    @Override
    public void save(Path path) throws IOException {
        internalIndex.save(path);
    }

    @Override
    public void load(Path path) throws IOException {
        internalIndex.load(path);
    }
}