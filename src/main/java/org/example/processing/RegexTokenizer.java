package org.example.processing;

import java.util.regex.Pattern;
import java.util.stream.Stream;

public class RegexTokenizer implements Tokenizer {
    private static final Pattern WORD_PATTERN =
            Pattern.compile("\\p{L}[\\p{L}'-]*\\p{L}|\\p{L}");

    @Override
    public Stream<String> tokenize(String text) {
        if (text == null || text.isBlank()) {
            return Stream.empty();
        }
        return WORD_PATTERN.matcher(text)
                .results()
                .map(matchResult -> matchResult.group().toLowerCase());
    }
}