package dev.langchain4j.store.embedding.index;

public class HNSWIndex implements VectorIndex {

    private final Integer m;
    private final Integer efConstruction;
    private final DistanceStrategy distanceStrategy;

    public HNSWIndex(Integer m, Integer efConstruction, DistanceStrategy distanceStrategy) {
        this.m = (m != null) ? m : 16;
        this.efConstruction = (efConstruction != null) ? efConstruction : 64;
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.COSINE_DISTANCE;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(m = %s, ef_construction = %s)", m, efConstruction);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}
