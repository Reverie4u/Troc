package troc.reducer;

public interface OracleChecker {
    // 判断测试用例是否会触发bug
    boolean hasBug(String tc);
}