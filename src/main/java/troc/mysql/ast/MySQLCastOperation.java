package troc.mysql.ast;

import java.util.Map;

public class MySQLCastOperation implements MySQLExpression {
    private final MySQLExpression expr;
    private final CastType type;

    public enum CastType {
        UNSIGNED, SIGNED;

        // public static CastType getRandom() {
        // return Randomly.fromOptions(CastType.values());
        // }
        public static CastType getRandom() {
            return SIGNED;
            // return Randomly.fromOptions(CastType.values());
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
    public MySQLConstant getExpectedValue(Map<String, Object> row) {
        return expr.getExpectedValue(row).castAs(type);
    }

}
