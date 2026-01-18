package org.example.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class JsonDictionaryWriter implements DictionaryWriter {
    private final ObjectMapper mapper;

    public JsonDictionaryWriter() {
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void write(Map<String, Integer> dictionary, Path destination) throws IOException {
        mapper.writeValue(destination.toFile(), dictionary);
    }
}