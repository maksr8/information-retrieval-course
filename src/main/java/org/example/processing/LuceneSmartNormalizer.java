package org.example.processing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern;


public class LuceneSmartNormalizer implements TermNormalizer {

    private final Analyzer enAnalyzer;
    private final Analyzer uaAnalyzer;

    private final Pattern cyrillicPattern = Pattern.compile("[а-яА-ЯґҐєЄіІїЇ]");

    public LuceneSmartNormalizer() {
        this.enAnalyzer = new EnglishAnalyzer();
        this.uaAnalyzer = new UkrainianMorfologikAnalyzer();
    }

    @Override
    public String normalize(String token) {
        if (token == null || token.isBlank()) return null;

        Analyzer analyzer = cyrillicPattern.matcher(token).find() ? uaAnalyzer : enAnalyzer;

        try {
            return processWithLucene(analyzer, token);
        } catch (Exception e) {
            return token.toLowerCase();
        }
    }

    private String processWithLucene(Analyzer analyzer, String text) throws IOException {

        try (TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(text))) {
            CharTermAttribute termAttr = tokenStream.addAttribute(CharTermAttribute.class);

            tokenStream.reset();

            if (tokenStream.incrementToken()) {
                return termAttr.toString();
            }
        }

        return null;
    }
}