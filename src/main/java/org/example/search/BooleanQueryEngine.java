package org.example.search;

import org.example.index.SearchIndex;
import org.example.processing.TermNormalizer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BooleanQueryEngine {
    private final SearchIndex index;
    private final TermNormalizer normalizer;

    private static final Map<String, Integer> PRIORITY = Map.of(
            "NOT", 3,
            "AND", 2,
            "OR", 1
    );

    public BooleanQueryEngine(SearchIndex index, TermNormalizer normalizer) {
        this.index = index;
        this.normalizer = normalizer;
    }

    public Set<Integer> search(String query) {
        if (query == null || query.isBlank()) return Collections.emptySet();

        List<String> tokens = tokenize(query);

        Queue<String> rpn = convertToRPN(tokens);

        return evaluateRPN(rpn);
    }

    private List<String> tokenize(String query) {
        String prepared = query.replace("(", " ( ")
                .replace(")", " ) ");

        return Arrays.stream(prepared.split("\\s+"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }

    private Queue<String> convertToRPN(List<String> tokens) {
        Queue<String> outputQueue = new LinkedList<>();
        Stack<String> operatorStack = new Stack<>();

        for (String token : tokens) {

            if (PRIORITY.containsKey(token)) {
                while (!operatorStack.isEmpty()
                        && !operatorStack.peek().equals("(")
                        && PRIORITY.getOrDefault(operatorStack.peek(), 0) >= PRIORITY.get(token)) {
                    outputQueue.add(operatorStack.pop());
                }
                operatorStack.push(token);
            } else if (token.equals("(")) {
                operatorStack.push("(");
            } else if (token.equals(")")) {
                while (!operatorStack.isEmpty() && !operatorStack.peek().equals("(")) {
                    outputQueue.add(operatorStack.pop());
                }
                operatorStack.pop();
            } else {
                outputQueue.add(token);
            }
        }

        while (!operatorStack.isEmpty()) {
            outputQueue.add(operatorStack.pop());
        }

        return outputQueue;
    }

    private Set<Integer> evaluateRPN(Queue<String> rpn) {
        Stack<SearchResult> stack = new Stack<>();

        for (String token : rpn) {
            switch (token) {
                case "AND" -> {
                    SearchResult right = stack.pop();
                    SearchResult left = stack.pop();

                    stack.push(left.and(right));
                }
                case "OR" -> {
                    SearchResult right = stack.pop();
                    SearchResult left = stack.pop();

                    stack.push(left.or(right));
                }
                case "NOT" -> {
                    SearchResult operand = stack.pop();

                    stack.push(operand.not(index.getDocCount()));
                }
                default -> {
                    String term = normalizer.normalize(token);
                    if (term == null) {
                        System.out.println("Ignored stop word: " + token);
                        stack.push(index.search(""));
                    } else {
                        stack.push(index.search(term));
                    }
                }
            }
        }

        return stack.isEmpty() ? Collections.emptySet() : stack.pop().toSet();
    }
}
