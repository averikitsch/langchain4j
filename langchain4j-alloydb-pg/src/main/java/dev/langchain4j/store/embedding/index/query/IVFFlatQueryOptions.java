package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class IVFFlatQueryOptions implements QueryOptions {

    private final Integer probes;

    public IVFFlatQueryOptions(Integer probes) {
        this.probes = (probes != null) ? probes : 1;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("ivfflat.probes = %d", probes));
        return parameters;
    }
}
