package troc.mysql.ast;

import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in">Comparison Functions and
 *      Operators</a>
 */
public class MySQLInOperation implements MySQLExpression {

    private final MySQLExpression expr;
    private final List<MySQLExpression> listElements;
    private final boolean isTrue;

    public MySQLInOperation(MySQLExpression expr, List<MySQLExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public List<MySQLExpression> getListElements() {
        return listElements;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;

    }

    public boolean isTrue() {
        return isTrue;
    }
}
