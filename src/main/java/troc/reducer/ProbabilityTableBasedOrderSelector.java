package troc.reducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProbabilityTableBasedOrderSelector<T> implements OrderSelector<T> {
    Map<T, Integer> candidatesMap;
    int initialWeight = 100;
    // 步长
    int step = 1;

    @Override
    public T selectNext(List<T> excludedList) {
        Map<T, Integer> candidatesMapCopy = new HashMap<T, Integer>(candidatesMap);
        // 从candidatesMapCopy中移除key在excludedList中的元素
        for (T key : excludedList) {
            candidatesMapCopy.remove(key);
        }
        if (candidatesMapCopy.isEmpty()) {
            return null;
        }
        // 使用轮盘赌方法选择下一个类型
        int sum = 0;
        // 遍历candidatesMapCopy，计算权重总和
        for (Map.Entry<T, Integer> entry : candidatesMapCopy.entrySet()) {
            sum += entry.getValue();
        }
        int rand = (int) (Math.random() * sum);
        for (Map.Entry<T, Integer> entry : candidatesMapCopy.entrySet()) {
            rand -= entry.getValue();
            if (rand <= 0) {
                return entry.getKey();
            }
        }
        return null;
    }
    @Override
    public T selectNext() {
        // 使用轮盘赌方法选择下一个类型
        int sum = 0;
        // 遍历candidatesMapCopy，计算权重总和
        for (Map.Entry<T, Integer> entry : candidatesMap.entrySet()) {
            sum += entry.getValue();
        }
        int rand = (int) (Math.random() * sum);
        for (Map.Entry<T, Integer> entry : candidatesMap.entrySet()) {
            rand -= entry.getValue();
            if (rand <= 0) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    public ProbabilityTableBasedOrderSelector(List<T> candidates) {
        candidatesMap = new HashMap<>();
        for (T candidate : candidates) {
            candidatesMap.put(candidate, initialWeight);
        }
    }

    @Override
    public void updateWeight(T candidate, boolean success) {
        int weight = 0;
        if (success) {
            weight = 1;
        } else {
            weight = -1;
        }
        // weight只能是-1.0或者1.0
        int newWeight = candidatesMap.get(candidate) + weight * step;
        candidatesMap.put(candidate, newWeight);
        // 更新其他的权重
        for (T key : candidatesMap.keySet()) {
            if (key != candidate) {
                int oldWeight = candidatesMap.get(key);
                int newWeight2 = oldWeight - weight * step;
                candidatesMap.put(key, newWeight2);
            }
        }
    }
}
