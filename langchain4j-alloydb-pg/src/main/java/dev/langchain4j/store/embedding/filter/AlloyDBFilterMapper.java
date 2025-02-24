package dev.langchain4j.store.embedding.filter;

import dev.langchain4j.store.embedding.filter.Filter;

import dev.langchain4j.store.embedding.filter.Filter;
import dev.langchain4j.store.embedding.filter.comparison.*;
import dev.langchain4j.store.embedding.filter.logical.And;
import dev.langchain4j.store.embedding.filter.logical.Not;
import dev.langchain4j.store.embedding.filter.logical.Or;

public class AlloyDBFilterMapper {

    public String map(Filter filter) {
        if (filter instanceof IsEqualTo) {
            return mapEqual((IsEqualTo) filter);
        } else if (filter instanceof IsNotEqualTo) {
            return mapNotEqual((IsNotEqualTo) filter);
        } else if (filter instanceof IsGreaterThan) {
            return mapGreaterThan((IsGreaterThan) filter);
        } else if (filter instanceof IsGreaterThanOrEqualTo) {
            return mapGreaterThanOrEqual((IsGreaterThanOrEqualTo) filter);
        } else if (filter instanceof IsLessThan) {
            return mapLessThan((IsLessThan) filter);
        } else if (filter instanceof IsLessThanOrEqualTo) {
            return mapLessThanOrEqual((IsLessThanOrEqualTo) filter);
        } else if (filter instanceof IsIn) {
            return mapIn((IsIn) filter);
        } else if (filter instanceof IsNotIn) {
            return mapNotIn((IsNotIn) filter);
        } else if (filter instanceof And) {
            return mapAnd((And) filter);
        } else if (filter instanceof Not) {
            return mapNot((Not) filter);
        } else if (filter instanceof Or) {
            return mapOr((Or) filter);
        } else {
            throw new UnsupportedOperationException("Unsupported filter type: " + filter.getClass().getName());
        }
    }

    private String mapEqual(IsEqualTo isEqualTo) {
        String key = isEqualTo.key();
        return String.format("\"%s\" IS NOT NULL AND \"%s\" = %s", key, key,
                formatValue(isEqualTo.comparisonValue()));
    }

    private String mapNotEqual(IsNotEqualTo isNotEqualTo) {
        String key = isNotEqualTo.key();
        return String.format("\"%s\" IS NULL OR \"%s\" != %s", key, key,
                formatValue(isNotEqualTo.comparisonValue()));
    }

    private String mapGreaterThan(IsGreaterThan isGreaterThan) {
        return String.format("\"%s\" > %s", isGreaterThan.key(),
                formatValue(isGreaterThan.comparisonValue()));
    }

    private String mapGreaterThanOrEqual(IsGreaterThanOrEqualTo isGreaterThanOrEqualTo) {
        return String.format("\"%s\" >= %s", isGreaterThanOrEqualTo.key(),
                formatValue(isGreaterThanOrEqualTo.comparisonValue()));
    }

    private String mapLessThan(IsLessThan isLessThan) {
        return String.format("\"%s\" < %s", isLessThan.key(),
                formatValue(isLessThan.comparisonValue()));
    }

    private String mapLessThanOrEqual(IsLessThanOrEqualTo isLessThanOrEqualTo) {
        return String.format("\"%s\" <= %s", isLessThanOrEqualTo.key(),
                formatValue(isLessThanOrEqualTo.comparisonValue()));
    }

    private String mapIn(IsIn isIn) {
        return String.format("\"%s\" IN %s", isIn.key(), formatValuesAsString(isIn.comparisonValues()));
    }

    private String mapNotIn(IsNotIn isNotIn) {
        String key = isNotIn.key();
        return String.format("\"%s\" IS NULL OR \"%s\" NOT IN %s", key, key, formatValuesAsString(isNotIn.comparisonValues()));
    }

    private String mapAnd(And and) {
        return String.format("%s AND %s", map(and.left()), map(and.right()));
    }

    private String mapNot(Not not) {
        return String.format("NOT(%s)", map(not.expression()));
    }

    private String mapOr(Or or) {
        return String.format("(%s OR %s)", map(or.left()), map(or.right()));
    }

    String formatValue(Object value) {
        if (value instanceof String || value instanceof UUID) {
            return "'" + value + "'";
        } else {
            return value.toString();
        }
    }

    String formatValuesAsString(Collection<?> values) {
        return "(" + values.stream().map(v -> String.format("'%s'", v))
                .collect(Collectors.joining(",")) + ")";
    }

}
