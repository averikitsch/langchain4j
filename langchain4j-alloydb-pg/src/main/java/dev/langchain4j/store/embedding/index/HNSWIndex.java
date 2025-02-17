package dev.langchain4j.store.embedding.index;

import java.util.ArrayList;
import java.util.List;

public class HNSWIndex implements BaseIndex {

    private final String indexType;
    private final Integer m;
    private final Integer efConstruction;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public HNSWIndex(Builder builder) {
        this.indexType = builder.getIndexType();
        this.m = builder.getM();
        this.efConstruction = builder.getEfConstruction();
        this.distanceStrategy = builder.getDistanceStrategy();
        this.partialIndexes = builder.getPartialIndexes();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(m = %s, ef_construction = %s)", m, efConstruction);
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }

    public String getIndexType() {
        return indexType;
    }

    public class Builder {

        private String indexType;
        private Integer m;
        private Integer efConstruction;
        private DistanceStrategy distanceStrategy;
        private List<String> partialIndexes;

        public Builder() {
            this.indexType = DEFAULT_INDEX_NAME_SUFFIX;
            this.m = 16;
            this.efConstruction = 64;
            this.distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.partialIndexes = new ArrayList<>();

        }

        public String getIndexType() {
            return indexType;
        }

        public Builder indexType(String indexType) {
            this.indexType = indexType;
            return this;
        }

        public Integer getM() {
            return m;
        }

        public Builder m(Integer m) {
            this.m = m;
            return this;
        }

        public Integer getEfConstruction() {
            return efConstruction;
        }

        public Builder efConstruction(Integer efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public DistanceStrategy getDistanceStrategy() {
            return distanceStrategy;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public List<String> getPartialIndexes() {
            return partialIndexes;
        }

        public Builder partialIndexes(List<String> partialIndexes) {
            this.partialIndexes = partialIndexes;
            return this;
        }

        public HNSWIndex build() {
            return new HNSWIndex(this);
        }
    }

}
