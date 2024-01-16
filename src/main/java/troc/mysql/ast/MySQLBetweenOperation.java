package troc.mysql.ast;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import troc.mysql.ast.MySQLBinaryComparisonOperation.BinaryComparisonOperator;
import troc.mysql.ast.MySQLBinaryLogicalOperation.MySQLBinaryLogicalOperator;

@Slf4j
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
    public MySQLConstant getExpectedValue(Map<String, Object> row) {
        MySQLExpression[] arr = { left, right, expr };
        MySQLConstant convertedExpr = MySQLComputableFunction.castToMostGeneralType(expr.getExpectedValue(row), row,
                arr);
        MySQLConstant convertedLeft = MySQLComputableFunction.castToMostGeneralType(left.getExpectedValue(row), row,
                arr);
        MySQLConstant convertedRight = MySQLComputableFunction.castToMostGeneralType(right.getExpectedValue(row), row,
                arr);

        /* workaround for https://bugs.mysql.com/bug.php?id=96006 */
        // if (convertedLeft.isInt() && convertedLeft.getInt() < 0 ||
        // convertedRight.isInt() && convertedRight.getInt() < 0
        // || convertedExpr.isInt() && convertedExpr.getInt() < 0) {
        // log.info("IgnoreMeException 1");
        // throw new IgnoreMeException();
        // }
        MySQLBinaryComparisonOperation leftComparison = new MySQLBinaryComparisonOperation(convertedLeft, convertedExpr,
                BinaryComparisonOperator.LESS_EQUALS);
        MySQLBinaryComparisonOperation rightComparison = new MySQLBinaryComparisonOperation(convertedExpr,
                convertedRight, BinaryComparisonOperator.LESS_EQUALS);
        MySQLBinaryLogicalOperation andOperation = new MySQLBinaryLogicalOperation(leftComparison, rightComparison,
                MySQLBinaryLogicalOperator.AND);
        return andOperation.getExpectedValue(row);
    }

}
