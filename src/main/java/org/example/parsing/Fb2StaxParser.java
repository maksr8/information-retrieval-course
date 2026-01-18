package org.example.parsing;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.function.Consumer;

public class Fb2StaxParser implements DocumentParser {
    private final XMLInputFactory factory;

    public Fb2StaxParser() {
        this.factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    }

    @Override
    public void parse(Path filePath, Consumer<String> textConsumer) {
        try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            boolean insideBody = false;
            boolean insideParagraph = false;

            while (reader.hasNext()) {
                int event = reader.next();

                switch (event) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String tagName = reader.getLocalName().toLowerCase();

                        if ("body".equals(tagName)) {
                            insideBody = true;
                        }
                        else if (insideBody && ("p".equals(tagName) || "v".equals(tagName))) {
                            insideParagraph = true;
                        }
                    }

                    case XMLStreamConstants.CHARACTERS -> {
                        if (insideBody && insideParagraph) {
                            String text = reader.getText();
                            if (!text.isBlank()) {
                                textConsumer.accept(text);
                            }
                        }
                    }

                    case XMLStreamConstants.END_ELEMENT -> {
                        String tagName = reader.getLocalName().toLowerCase();

                        if ("body".equals(tagName)) {
                            insideBody = false;
                        }
                        else if ("p".equals(tagName) || "v".equals(tagName)) {
                            insideParagraph = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error in file " + filePath.getFileName() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}