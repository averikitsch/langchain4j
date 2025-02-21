package dev.langchain4j.store.embedding.index;

import java.util.List;

public class ScaNNIndex implements BaseIndex {

    private final String indexType = "ScaNN";
    private final String name;
    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public ScaNNIndex(Builder builder) {
        this.name = builder.name;
        this.numLeaves = builder.numLeaves;
        this.quantizer = builder.quantizer;
        this.distanceStrategy = builder.distanceStrategy;
        this.partialIndexes = builder.partialIndexes;
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    public String getIndexType() {
        return indexType;
    }

    public String getName() {
        return name;
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }

    public class Builder {
        private String name;
        private Integer numLeaves = 5;
        private String quantizer = "sq8";
        private DistanceStrategy distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
        private List<String> partialIndexes;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder numLeaves(Integer numLeaves) {
            this.numLeaves = numLeaves;
            return this;
        }

        public Builder quantizer(String quantizer) {
            this.quantizer = quantizer;
            return this;
        }

        public Builder distanceStrategy(DistanceStrategy distanceStrategy) {
            this.distanceStrategy = distanceStrategy;
            return this;
        }

        public Builder partialIndexes(List<String> partialIndexes) {
            this.partialIndexes = partialIndexes;
            return this;
        }

        public ScaNNIndex build() {
            return new ScaNNIndex(this);
        }

    }
}
