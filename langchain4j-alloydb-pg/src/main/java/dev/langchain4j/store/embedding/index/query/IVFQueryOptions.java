package dev.langchain4j.store.embedding.index.query;

import java.util.ArrayList;
import java.util.List;

public class IVFQueryOptions implements QueryOptions {

    private final Integer probes;

    public IVFQueryOptions(Integer probes) {
        this.probes = (probes != null) ? probes : 1;
    }

    @Override
    public List<String> getParameterSettings() {
        List<String> parameters = new ArrayList();
        parameters.add(String.format("ivf.probes = %d", probes));
        return parameters;
    }
}
