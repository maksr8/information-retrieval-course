package org.example.index;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class DocumentRegistry implements DocRegistry{
    private final List<String> docNames = new ArrayList<>();

    public int registerDoc(String docName) {
        docNames.add(docName);
        return docNames.size() - 1;
    }

    public String getDocName(int docId) {
        if (docId < 0 || docId >= docNames.size()) {
            throw new IllegalArgumentException("Invalid docId: " + docId + ". Valid range is 0 to " + (docNames.size() - 1));
        }
        return docNames.get(docId);
    }

    public int getDocCount() {
        return docNames.size();
    }

    public void save(Path path) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path.toFile())))) {
            out.writeInt(docNames.size());
            for (String docName : docNames) {
                out.writeUTF(docName);
            }
        }
    }

    public void load(Path path) throws IOException {
        docNames.clear();
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())))) {
            int docCount = in.readInt();
            for (int i = 0; i < docCount; i++) {
                docNames.add(in.readUTF());
            }
        }
    }
}