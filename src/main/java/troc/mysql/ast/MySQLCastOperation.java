package troc.mysql.ast;

import troc.Randomly;

public class MySQLCastOperation implements MySQLExpression {

    // 和troc不太一样
    private final MySQLExpression expr;
    private final CastType type;

    public enum CastType {
        SIGNED, FLOAT, DOUBLE, CHAR;

        public static CastType getRandom() {
            return Randomly.fromOptions(CastType.values());
        }

    }

    public MySQLCastOperation(MySQLExpression expr, CastType type) {
        this.expr = expr;
        this.type = type;
    }

    public MySQLExpression getExpr() {
        return expr;
    }

    public CastType getType() {
        return type;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

}
