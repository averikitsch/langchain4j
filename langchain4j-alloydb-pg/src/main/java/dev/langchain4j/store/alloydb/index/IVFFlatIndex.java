package dev.langchain4j.store.alloydb.index;

import java.util.ArrayList;
import java.util.List;

public class IVFFlatIndex implements VectorIndex {

    private final Integer listCount;
    private final Integer probes;
    private final DistanceStrategy distanceStrategy;

    public IVFFlatIndex(Integer listCount, Integer probes, DistanceStrategy distanceStrategy) {
        this.listCount = (listCount != null) ? listCount : 1;
        this.probes = (probes != null) ? probes : 1;
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.DistanceStrategyFactory.getCosineDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s)", listCount);
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("ivfflat.probes = %d", probes));
        return parameters;
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }
}
