package troc.reducer;

import java.util.List;

public interface OrderSelector<T> {
    // 选择下一个测试用例
    public T selectNext(List<T> excludedList);
}