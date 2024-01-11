package troc.mysql.ast;

import troc.Randomly;

public class MySQLBinaryComparisonOperation implements MySQLExpression {

    public enum BinaryComparisonOperator {
        EQUALS("=") {
            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }
        },
        LESS("<") {

            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }
        },
        GREATER(">") {
            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }

        },
        LIKE("LIKE") {

            @Override
            public MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal) {
                return null;
            }

        };

        private final String textRepresentation;

        public String getTextRepresentation() {
            return textRepresentation;
        }

        BinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract MySQLConstant getExpectedValue(MySQLConstant leftVal, MySQLConstant rightVal);

        public static BinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(BinaryComparisonOperator.values());
        }
    }

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final BinaryComparisonOperator op;

    public MySQLBinaryComparisonOperation(MySQLExpression left, MySQLExpression right, BinaryComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public BinaryComparisonOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return op.getExpectedValue(left.getExpectedValue(), right.getExpectedValue());
    }

}
