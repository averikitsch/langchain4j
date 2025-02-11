package dev.langchain4j.store.embedding.index;

public class ScaNNIndex implements VectorIndex {

    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public ScaNNIndex(IndexConfig indexConfig) {
        this.numLeaves = indexConfig.getNumLeaves();
        this.quantizer = indexConfig.getQuantizer();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
