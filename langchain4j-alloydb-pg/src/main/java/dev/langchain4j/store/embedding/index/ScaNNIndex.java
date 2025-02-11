package dev.langchain4j.store.embedding.index;

import java.util.List;

public class ScaNNIndex implements VectorIndex {

    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public ScaNNIndex(IndexConfig indexConfig) {
        this.numLeaves = indexConfig.getNumLeaves();
        this.quantizer = indexConfig.getQuantizer();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
        this.partialIndexes = indexConfig.getPartialIndexes();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }
}
