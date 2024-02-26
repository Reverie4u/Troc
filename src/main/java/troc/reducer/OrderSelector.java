package troc.reducer;

public interface OrderSelector<T> {
    // 选择下一个测试用例
    public T selectNext();

    public void removeCandidate(T candidate);
}