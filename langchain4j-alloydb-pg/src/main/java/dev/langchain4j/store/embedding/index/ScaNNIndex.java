package dev.langchain4j.store.embedding.index;

public class ScaNNIndex implements VectorIndex {

    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public ScaNNIndex(Integer numLeaves, String quantizer, Integer num_leaves_to_search, Integer pre_reordering_num_neighbors, DistanceStrategy distanceStrategy) {
        this.numLeaves = (numLeaves != null) ? numLeaves : 5;
        this.quantizer = (quantizer != null) ? quantizer : "sq8";
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.COSINE_DISTANCE;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
