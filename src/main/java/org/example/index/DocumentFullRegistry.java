package org.example.index;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DocumentFullRegistry {
    private final List<DocumentMetadata> documents = new ArrayList<>();

    public int registerDoc(String gutenbergId, String title, String author) {
        int docId = documents.size();
        documents.add(new DocumentMetadata(docId, gutenbergId, title, author));
        return docId;
    }

    public DocumentMetadata getMetadata(int docId) {
        if (docId < 0 || docId >= documents.size()) {
            throw new IllegalArgumentException("Invalid docId: " + docId);
        }
        return documents.get(docId);
    }

    public int getDocCount() {
        return documents.size();
    }

    public void save(Path path) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeInt(documents.size());
            for (DocumentMetadata meta : documents) {
                out.writeUTF(meta.gutenbergId());
                out.writeUTF(meta.title() != null ? meta.title() : "Unknown Title");
                out.writeUTF(meta.author() != null ? meta.author() : "Unknown Author");
            }
        }
    }

    public void load(Path path) throws IOException {
        documents.clear();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int docCount = in.readInt();
            for (int i = 0; i < docCount; i++) {
                String gutenbergId = in.readUTF();
                String title = in.readUTF();
                String author = in.readUTF();
                documents.add(new DocumentMetadata(i, gutenbergId, title, author));
            }
        }
    }
}