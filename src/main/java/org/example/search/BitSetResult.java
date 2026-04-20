package org.example.search;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BitSetResult implements SearchResult {
    private final BitSet bitSet;

    public BitSetResult(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Override
    public SearchResult and(SearchResult other) {
        BitSet otherBits = ((BitSetResult) other).bitSet;

        BitSet result = (BitSet) this.bitSet.clone();
        result.and(otherBits);
        return new BitSetResult(result);
    }

    @Override
    public SearchResult or(SearchResult other) {
        BitSet otherBits = ((BitSetResult) other).bitSet;
        BitSet result = (BitSet) this.bitSet.clone();
        result.or(otherBits);
        return new BitSetResult(result);
    }

    @Override
    public SearchResult not(int totalDocs) {
        BitSet result = (BitSet) this.bitSet.clone();
        result.flip(0, totalDocs);
        return new BitSetResult(result);
    }

    @Override
    public List<Integer> toList() {
        List<Integer> list = new ArrayList<>();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            list.add(i);
        }
        return list;
    }
}
