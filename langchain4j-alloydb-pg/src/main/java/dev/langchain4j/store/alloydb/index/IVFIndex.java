package dev.langchain4j.store.alloydb.index;

import java.util.ArrayList;
import java.util.List;

public class IVFIndex implements VectorIndex {

    private final Integer listCount;
    private final Integer probes;
    private final String quantizer;
    private final DistanceStrategy distanceStrategy;

    public IVFIndex(Integer listCount, Integer probes, String quantizer, DistanceStrategy distanceStrategy) {
        this.listCount = (listCount != null) ? listCount : 100;
        this.probes = (probes != null) ? probes : 1;
        this.quantizer = (quantizer != null) ? quantizer : "sq8";
        this.distanceStrategy = distanceStrategy != null ? distanceStrategy : DistanceStrategy.DistanceStrategyFactory.getCosineDistanceStrategy();
    }

    @Override
    public String getIndexOptions() {
        return String.format("(lists = %s, quantizer = %s)", listCount, quantizer);
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("ivf.probes = %d", probes));
        return parameters;
    }

    public DistanceStrategy getDistanceStrategy() {
        return distanceStrategy;
    }

}
