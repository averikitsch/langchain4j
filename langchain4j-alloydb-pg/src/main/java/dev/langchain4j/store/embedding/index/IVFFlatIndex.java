package dev.langchain4j.store.embedding.index;

public class IVFFlatIndex implements VectorIndex {

    private final Integer listCount;
    private final DistanceStrategy distanceStrategy;

    public IVFFlatIndex(Integer listCount, DistanceStrategy distanceStrategy) {
        this.listCount = (listCount != null) ? listCount : 1;
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.COSINE_DISTANCE;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s)", listCount);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}
