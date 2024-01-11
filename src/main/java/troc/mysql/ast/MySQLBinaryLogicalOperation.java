package troc.mysql.ast;

import troc.Randomly;

public class MySQLBinaryLogicalOperation implements MySQLExpression {

    private final MySQLExpression left;
    private final MySQLExpression right;
    private final MySQLBinaryLogicalOperator op;
    private final String textRepresentation;

    public enum MySQLBinaryLogicalOperator {
        AND("AND", "&&") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return null;
            }
        },
        OR("OR", "||") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return null;
            }
        },
        XOR("XOR") {
            @Override
            public MySQLConstant apply(MySQLConstant left, MySQLConstant right) {
                return null;
            }
        };

        private final String[] textRepresentations;

        MySQLBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public abstract MySQLConstant apply(MySQLConstant left, MySQLConstant right);

        public static MySQLBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public MySQLBinaryLogicalOperation(MySQLExpression left, MySQLExpression right, MySQLBinaryLogicalOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
        this.textRepresentation = op.getTextRepresentation();
    }

    public MySQLExpression getLeft() {
        return left;
    }

    public MySQLBinaryLogicalOperator getOp() {
        return op;
    }

    public MySQLExpression getRight() {
        return right;
    }

    public String getTextRepresentation() {
        return textRepresentation;
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

}
