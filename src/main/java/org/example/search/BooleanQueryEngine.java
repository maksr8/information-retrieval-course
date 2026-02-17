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
        Stack<Set<Integer>> stack = new Stack<>();

        for (String token : rpn) {
            switch (token) {
                case "AND" -> {
                    Set<Integer> right = stack.pop();
                    Set<Integer> left = stack.pop();

                    Set<Integer> result = new HashSet<>(left);
                    result.retainAll(right);
                    stack.push(result);
                }
                case "OR" -> {
                    Set<Integer> right = stack.pop();
                    Set<Integer> left = stack.pop();

                    Set<Integer> result = new HashSet<>(left);
                    result.addAll(right);
                    stack.push(result);
                }
                case "NOT" -> {
                    Set<Integer> operand = stack.pop();

                    Set<Integer> result = getAllDocIds();
                    result.removeAll(operand);
                    stack.push(result);
                }
                default -> {
                    String term = normalizer.normalize(token);
                    if (term == null) {
                        System.out.println("Ignored stop word: " + token);
                        stack.push(new HashSet<>());
                    } else {
                        System.out.println("Searching for: '" + token + "' -> normalized: '" + term + "'");
                        Set<Integer> docs = index.search(term);
                        stack.push(docs == null ? new HashSet<>() : new HashSet<>(docs));
                    }
                }
            }
        }

        return stack.isEmpty() ? Collections.emptySet() : stack.pop();
    }

    private Set<Integer> getAllDocIds() {
        int count = index.getDocCount();
        return IntStream.range(0, count).boxed().collect(Collectors.toSet());
    }
}
