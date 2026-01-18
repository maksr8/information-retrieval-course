package org.example.parsing;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.function.Consumer;

public class Fb2StaxParser implements DocumentParser {
    private final XMLInputFactory factory;
    private static final int MAX_PARAGRAPH_SIZE = 100 * 1024;

    public Fb2StaxParser() {
        this.factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    }

    @Override
    public void parse(Path filePath, Consumer<String> textConsumer) {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            StringBuilder buffer = new StringBuilder();
            boolean insideBody = false;
            boolean insideParagraph = false;

            while (reader.hasNext()) {
                int event = reader.next();

                switch (event) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String tagName = reader.getLocalName().toLowerCase();
                        if ("body".equals(tagName)) {
                            insideBody = true;
                        } else if (insideBody && ("p".equals(tagName) || "v".equals(tagName))) {
                            insideParagraph = true;
                            buffer.setLength(0);
                        }
                    }

                    case XMLStreamConstants.CHARACTERS -> {
                        if (insideParagraph) {
                            String textPart = reader.getText();

                            if (buffer.length() + textPart.length() > MAX_PARAGRAPH_SIZE) {
                                System.err.println("WARN: Paragraph exceeded max size, truncating.");
                                insideParagraph = false;
                            } else {
                                buffer.append(textPart);
                            }
                        }
                    }

                    case XMLStreamConstants.END_ELEMENT -> {
                        String tagName = reader.getLocalName().toLowerCase();
                        if ("body".equals(tagName)) {
                            insideBody = false;
                        } else if ("p".equals(tagName) || "v".equals(tagName)) {
                            insideParagraph = false;
                            if (!buffer.isEmpty()) {
                                textConsumer.accept(buffer.toString());
                                buffer.setLength(0);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing file " + filePath + ": " + e.getMessage());
        }
    }
}