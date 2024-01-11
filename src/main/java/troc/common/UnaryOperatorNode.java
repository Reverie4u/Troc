package troc.common;

import troc.common.BinaryOperatorNode.Operator;

public abstract class UnaryOperatorNode<T, O extends Operator> extends UnaryNode<T> {

    protected final O op;

    protected UnaryOperatorNode(T expr, O op) {
        super(expr);
        this.op = op;
    }

    @Override
    public String getOperatorRepresentation() {
        return op.getTextRepresentation();
    }

}
