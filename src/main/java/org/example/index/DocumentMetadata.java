package org.example.index;

public record DocumentMetadata(
    int docId, 
    String gutenbergId, 
    String title, 
    String author
) {}