package troc.mysql.ast;

import java.util.function.BinaryOperator;

import troc.Randomly;

public class MySQLBinaryOperation implements MySQLExpression {

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final MySQLBinaryOperator op;

    public enum MySQLBinaryOperator {

        AND("&") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l & r);
            }

        },
        OR("|") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l | r);
            }
        },
        XOR("^") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return applyBitOperation(left, right, (l, r) -> l ^ r);
            }
        };

        private String textRepresentation;

        private static MySQLConstant applyBitOperation(MySQLConstant left, MySQLConstant right,
                BinaryOperator<Long> op) {
            return null;
        }

        MySQLBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract MySQLConstant apply(MySQLConstant left, MySQLConstant right);

        public static MySQLBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public MySQLBinaryOperation(MySQLExpression left, MySQLExpression right, MySQLBinaryOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public MySQLBinaryOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

}
