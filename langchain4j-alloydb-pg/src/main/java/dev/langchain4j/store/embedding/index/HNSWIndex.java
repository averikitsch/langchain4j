package dev.langchain4j.store.embedding.index;

public class HNSWIndex implements VectorIndex {

    private final Integer m;
    private final Integer efConstruction;
    private final DistanceStrategy distanceStrategy;

    public HNSWIndex(IndexConfig indexConfig) {
        this.m = indexConfig.getM();
        this.efConstruction = indexConfig.getEfConstruction();
        this.distanceStrategy = indexConfig.getDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(m = %s, ef_construction = %s)", m, efConstruction);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}
