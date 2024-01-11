package troc.mysql.ast;

public class MySQLBetweenOperation implements MySQLExpression {

    private final MySQLExpression expr;
    private final MySQLExpression left;
    private final MySQLExpression right;

    public MySQLBetweenOperation(MySQLExpression expr, MySQLExpression left, MySQLExpression right) {
        this.expr = expr;
        this.left = left;
        this.right = right;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public MySQLExpression getRight() {
        return right;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

}
