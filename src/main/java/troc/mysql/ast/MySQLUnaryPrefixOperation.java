package troc.mysql.ast;

import troc.Randomly;
import troc.common.BinaryOperatorNode.Operator;
import troc.common.UnaryOperatorNode;
import troc.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

public class MySQLUnaryPrefixOperation extends UnaryOperatorNode<MySQLExpression, MySQLUnaryPrefixOperator>
        implements MySQLExpression {

    public enum MySQLUnaryPrefixOperator implements Operator {
        NOT("!", "NOT") {
            @Override
            public MySQLConstant applyNotNull(MySQLConstant expr) {
                return null;
            }
        },
        PLUS("+") {
            @Override
            public MySQLConstant applyNotNull(MySQLConstant expr) {
                return expr;
            }
        },
        MINUS("-") {
            @Override
            public MySQLConstant applyNotNull(MySQLConstant expr) {
                return null;
            }
        };

        private String[] textRepresentations;

        MySQLUnaryPrefixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract MySQLConstant applyNotNull(MySQLConstant expr);

        public static MySQLUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }
    }

    public MySQLUnaryPrefixOperation(MySQLExpression expr, MySQLUnaryPrefixOperator op) {
        super(expr, op);
    }

    @Override
    public MySQLConstant getExpectedValue() {
        return null;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
