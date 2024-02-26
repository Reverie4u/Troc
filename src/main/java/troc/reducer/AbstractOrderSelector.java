package troc.reducer;

import java.util.Map;

public abstract class AbstractOrderSelector<T> implements OrderSelector<T> {
    Map<T, Double> candidateProfitMap;

    @Override
    public T selectNext() {
        throw new AssertionError("Not implement For AbstractOrderSelector");
    }

    @Override
    public void removeCandidate(T candidate) {
        if (candidateProfitMap != null) {
            candidateProfitMap.remove(candidate);
        }
    }
}
