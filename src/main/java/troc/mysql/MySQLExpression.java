package troc.mysql;

public interface MySQLExpression {
    default Object getExpectedValue() {
        throw new AssertionError("Not supported for this operator");
    }
}
