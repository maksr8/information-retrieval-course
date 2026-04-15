package org.example.search;

import java.util.ArrayList;
import java.util.List;

public class ListResult implements SearchResult {
    private final List<Integer> docs;

    public ListResult(List<Integer> docs) {
        this.docs = docs;
    }

    @Override
    public SearchResult and(SearchResult other) {
        List<Integer> otherDocs = other.toList();
        List<Integer> result = new ArrayList<>();
        int i = 0, j = 0;

        while (i < docs.size() && j < otherDocs.size()) {
            int a = docs.get(i);
            int b = otherDocs.get(j);
            if (a == b) {
                result.add(a);
                i++;
                j++;
            } else if (a < b) {
                i++;
            } else {
                j++;
            }
        }
        return new ListResult(result);
    }

    @Override
    public SearchResult or(SearchResult other) {
        List<Integer> otherDocs = other.toList();
        List<Integer> result = new ArrayList<>();
        int i = 0, j = 0;

        while (i < docs.size() || j < otherDocs.size()) {
            if (i == docs.size()) {
                result.add(otherDocs.get(j++));
            } else if (j == otherDocs.size()) {
                result.add(docs.get(i++));
            } else {
                int a = docs.get(i);
                int b = otherDocs.get(j);
                if (a == b) {
                    result.add(a);
                    i++;
                    j++;
                } else if (a < b) {
                    result.add(a);
                    i++;
                } else {
                    result.add(b);
                    j++;
                }
            }
        }
        return new ListResult(result);
    }

    @Override
    public SearchResult not(int totalDocs) {
        List<Integer> result = new ArrayList<>();
        int i = 0;
        for (int doc = 0; doc < totalDocs; doc++) {
            if (i < docs.size() && docs.get(i) == doc) {
                i++;
            } else {
                result.add(doc);
            }
        }
        return new ListResult(result);
    }

    @Override
    public List<Integer> toList() {
        return docs;
    }
}
