package dev.langchain4j.store.embedding.index;

import java.util.ArrayList;
import java.util.List;

public class ScaNNIndex implements BaseIndex {

    private final String indexType;
    private final Integer numLeaves;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;
    private final List<String> partialIndexes;

    public ScaNNIndex(Builder builder) {
        this.indexType = builder.getIndexType();
        this.numLeaves = builder.getNumLeaves();
        this.quantizer = builder.getQuantizer();
        this.distanceStrategy = builder.getDistanceStrategy();
        this.partialIndexes = builder.getPartialIndexes();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    public String getIndexType() {
        return indexType;
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

    public List<String> getPartialIndexes() {
        return partialIndexes;
    }

    public Integer getNumLeaves() {
        return numLeaves;
    }

    public String getQuantizer() {
        return quantizer;
    }

    public class Builder {

        private String indexType;
        private Integer numLeaves;
        private String quantizer;
        private DistanceStrategy distanceStrategy;
        private List<String> partialIndexes;

        public Builder() {
            this.indexType = DEFAULT_INDEX_NAME_SUFFIX;
            this.numLeaves = 5;
            this.quantizer = "sq8";
            this.distanceStrategy = DistanceStrategy.COSINE_DISTANCE;
            this.partialIndexes = new ArrayList<>();
        }

        public Builder indexType(String indexType) {
            this.indexType = indexType;
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

        public Integer getNumLeaves() {
            return this.numLeaves;
        }

        public String getQuantizer() {
            return this.quantizer;
        }

        public String getIndexType() {
            return indexType;
        }

        public DistanceStrategy getDistanceStrategy() {
            return distanceStrategy;
        }

        public List<String> getPartialIndexes() {
            return partialIndexes;
        }

    }
}
