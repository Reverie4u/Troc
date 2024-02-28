package troc.reducer;

import java.util.ArrayList;
import java.util.List;

import troc.Randomly;

public class RandomOrderSelector<T> implements OrderSelector<T> {
    List<T> candidates;

    // 构造方法
    public RandomOrderSelector(List<T> candidates) {
        this.candidates = candidates;
    }

    @Override
    public T selectNext(List<T> excludedList) {
        // 将candidates拷贝一份
        List<T> candidatesCopy = new ArrayList<T>(candidates);
        // 构造一个在candidates中排除excludedList后的新列表
        candidatesCopy.removeAll(excludedList);
        // 从candidatesCopy中随机选择一个元素返回
        return Randomly.fromList(candidatesCopy);
    }
}
