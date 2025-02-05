package dev.langchain4j.store.alloydb.index;

import java.util.ArrayList;
import java.util.List;

public class ScaNNIndex implements VectorIndex {

    private final Integer numLeaves;
    private final String quantizer;
    private final Integer num_leaves_to_search;
    private final Integer pre_reordering_num_neighbors;
    private final DistanceStrategy distanceStrategy;

    public ScaNNIndex(Integer numLeaves, String quantizer, Integer num_leaves_to_search, Integer pre_reordering_num_neighbors, DistanceStrategy distanceStrategy) {
        this.numLeaves = (numLeaves != null) ? numLeaves : 5;
        this.num_leaves_to_search = (num_leaves_to_search != null) ? num_leaves_to_search : 1;
        this.pre_reordering_num_neighbors = (pre_reordering_num_neighbors != null) ? pre_reordering_num_neighbors : -1;
        this.quantizer = (quantizer != null) ? quantizer : "sq8";
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.DistanceStrategyFactory.getCosineDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(num_leaves = %s, quantizer = %s)", numLeaves, quantizer);
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("scann.num_leaves_to_search = %s", num_leaves_to_search));
        parameters.add(String.format("scann.pre_reordering_num_neighbors = %s", pre_reordering_num_neighbors));
        return parameters;
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
