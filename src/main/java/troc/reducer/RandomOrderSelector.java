package troc.reducer;

import java.util.List;

import troc.Randomly;

public class RandomOrderSelector<T> implements OrderSelector<T> {
    List<T> candidates;

    // 构造方法
    public RandomOrderSelector(List<T> candidates) {
        this.candidates = candidates;
    }

    @Override
    public void removeCandidate(T candidate) {
        // 遍历candidates，找到candidate并删除
        for (int i = 0; i < candidates.size(); i++) {
            if (candidates.get(i).equals(candidate)) {
                candidates.remove(i);
                break;
            }
        }
    }

    @Override
    public T selectNext() {
        // 从candidates中随机选择一个元素返回
        return Randomly.fromList(candidates);
    }
}
