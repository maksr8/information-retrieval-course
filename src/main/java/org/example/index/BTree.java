package org.example.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A B-Tree implementation in Java. This implementation supports insertion and search operations, as well as a generic range search.
 * Doesn't support deletion.
 * @param <K>
 * @param <V>
 */
public class BTree<K extends Comparable<K>, V> {
    private final int t; // minimum degree / minimum number of children for a node (except root)
    // this also means each node can have at most 2t - 1 keys and 2t children
    private Node root;

    private class Node {
        int n; // number of keys currently in the node
        K[] keys;
        V[] values;
        Node[] children;
        boolean leaf;

        Node(boolean leaf) {
            this.leaf = leaf;
            this.keys = (K[]) new Comparable[2 * t - 1];
            this.values = (V[]) new Object[2 * t - 1];
            this.children = new BTree.Node[2 * t];
            this.n = 0;
        }
    }

    public BTree(int t) {
        if (t < 2) throw new IllegalArgumentException("Degree t must be at least 2");
        this.t = t;
        this.root = new Node(true);
    }

    /**
     * Searches for the value associated with the given key in the B-Tree.
     * @param key the key to search for
     * @return the value associated with the key, or null if the key is not found
     */
    public V get(K key) {
        return search(root, key);
    }

    private V search(Node x, K key) {
        // binarySearch from 0 to x.n exclusive
        int idx = Arrays.binarySearch(x.keys, 0, x.n, key);

        if (idx >= 0) {
            return x.values[idx];
        }

        int i = -(idx + 1); // if key is not found, binarySearch returns (-(insertion point) - 1)

        if (x.leaf) {
            return null;
        }
        return search(x.children[i], key); // recurse into the appropriate child
        // this is the appropriate child because if key is less than keys[i], it belongs in children[i], otherwise it belongs in children[i+1]
    }

    public void put(K key, V value) {
        if (updateIfExists(root, key, value)) {
            return;
        }
        // if we are here, it means the key does not exist, and we need to insert it

        //make sure the root is not full before inserting
        //this is a top-down build approach, we split full nodes on the way down to ensure we never have to split on the way back up
        Node r = root;
        if (r.n == 2 * t - 1) {
            Node s = new Node(false);
            root = s;
            s.children[0] = r;
            splitChild(s, 0);
            insertNonFull(s, key, value);
        } else {
            insertNonFull(r, key, value);
        }
    }

    /**
     * Searches for the key and updates its value if it exists.
     * @param x the current node being searched
     * @param key the key to search for
     * @param value the new value to associate with the key if it is found
     * @return true if the key was found and updated, false if the key does not exist in the tree
     */
    private boolean updateIfExists(Node x, K key, V value) {
        if (x == null)
            return false;

        int idx = Arrays.binarySearch(x.keys, 0, x.n, key);

        if (idx >= 0) {
            x.values[idx] = value;
            return true;
        }

        if (x.leaf) {
            return false;
        }

        int i = -(idx + 1);
        return updateIfExists(x.children[i], key, value);
    }

    /**
     * Inserts a key-value pair into a node that is guaranteed to have space for it (i.e., it is not full).
     * @param x the node into which the key-value pair should be inserted
     * @param key the key to insert
     * @param value the value to associate with the key
     */
    private void insertNonFull(Node x, K key, V value) {
        // idx will be not found, because we checked that before in put
        int idx = Arrays.binarySearch(x.keys, 0, x.n, key);
        int insertPos = -(idx + 1);

        if (x.leaf) {
            // shift keys and values to the right to make space for the new key
            for (int j = x.n - 1; j >= insertPos; j--) {
                x.keys[j + 1] = x.keys[j];
                x.values[j + 1] = x.values[j];
            }
            x.keys[insertPos] = key;
            x.values[insertPos] = value;
            x.n++;
        } else {
            // if full
            if (x.children[insertPos].n == 2 * t - 1) {
                splitChild(x, insertPos);
                // after splitting, the middle key of the full child moves up to x
                // we need to determine we go to the left or right of this new key
                if (key.compareTo(x.keys[insertPos]) > 0) {
                    insertPos++;
                }
            }
            insertNonFull(x.children[insertPos], key, value);
        }
    }

    /**
     * Splits a full child node into two nodes and promotes the middle key to the parent.
     * @param parent the parent node that will receive the promoted key
     * @param i the index of the child to split
     */
    private void splitChild(Node parent, int i) {
        Node fullChild = parent.children[i];

        Node newNode = new Node(fullChild.leaf);
        newNode.n = t - 1;

        // copy the right half of fullChild's keys and values to newNode
        for (int j = 0; j < t - 1; j++) {
            newNode.keys[j] = fullChild.keys[j + t];
            newNode.values[j] = fullChild.values[j + t];
        }

        if (!fullChild.leaf) {
            for (int j = 0; j < t; j++) {
                newNode.children[j] = fullChild.children[j + t];
            }
        }

        fullChild.n = t - 1;

        // shift parent's children to the right to make room for newNode
        for (int j = parent.n; j >= i + 1; j--) {
            parent.children[j + 1] = parent.children[j];
        }
        parent.children[i + 1] = newNode; // insert right part node

        // shift parent's keys to the right to make room for the promoted key
        for (int j = parent.n - 1; j >= i; j--) {
            parent.keys[j + 1] = parent.keys[j];
            parent.values[j + 1] = parent.values[j];
        }

        // move the middle key (index t - 1) up to the parent
        parent.keys[i] = fullChild.keys[t - 1];
        parent.values[i] = fullChild.values[t - 1];

        // Clear references
        fullChild.keys[t - 1] = null;
        fullChild.values[t - 1] = null;

        parent.n++;
    }

    /**
     * Performs a range search for keys between 'start' and 'end' (inclusive).
     * @param start the lower bound of the key range
     * @param end the upper bound of the key range
     * @return a list of keys in the B-Tree that fall within the specified range
     */
    public List<K> rangeSearch(K start, K end) {
        List<K> result = new ArrayList<>();
        rangeSearch(root, start, end, result);
        return result;
    }

    private void rangeSearch(Node x, K start, K end, List<K> result) {
        // use bin search to find start quicker
        int idx = Arrays.binarySearch(x.keys, 0, x.n, start);
        int i = (idx >= 0) ? idx : -(idx + 1);

        for (; i < x.n; i++) {
            if (x.keys[i].compareTo(end) > 0) {
                if (!x.leaf) rangeSearch(x.children[i], start, end, result);
                return;
            }

            if (!x.leaf) {
                rangeSearch(x.children[i], start, end, result);
            }
            result.add(x.keys[i]);
        }

        if (!x.leaf) {
            rangeSearch(x.children[i], start, end, result);
        }
    }
}