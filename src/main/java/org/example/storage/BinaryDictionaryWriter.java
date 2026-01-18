package org.example.storage;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class BinaryDictionaryWriter implements DictionaryWriter {
    @Override
    public void write(Map<String, Integer> dictionary, Path destination) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(destination.toFile())))) {

            dos.writeInt(dictionary.size());

            for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeInt(entry.getValue());
            }
        }
    }
}