package troc.mysql.visitor;

import troc.mysql.ast.MySQLBetweenOperation;
import troc.mysql.ast.MySQLBinaryComparisonOperation;
import troc.mysql.ast.MySQLBinaryLogicalOperation;
import troc.mysql.ast.MySQLBinaryOperation;
import troc.mysql.ast.MySQLCastOperation;
import troc.mysql.ast.MySQLColumnReference;
import troc.mysql.ast.MySQLConstant;
import troc.mysql.ast.MySQLExpression;
import troc.mysql.ast.MySQLInOperation;
import troc.mysql.ast.MySQLStringExpression;
import troc.mysql.ast.MySQLUnaryPostfixOperation;

public interface MySQLVisitor {

    void visit(MySQLConstant constant);

    void visit(MySQLColumnReference column);

    void visit(MySQLUnaryPostfixOperation column);

    void visit(MySQLBinaryLogicalOperation op);

    void visit(MySQLBinaryComparisonOperation op);

    void visit(MySQLCastOperation op);

    void visit(MySQLInOperation op);

    void visit(MySQLBinaryOperation op);

    void visit(MySQLStringExpression op);

    void visit(MySQLBetweenOperation op);

    default void visit(MySQLExpression expr) {
        if (expr instanceof MySQLConstant) {
            visit((MySQLConstant) expr);
        } else if (expr instanceof MySQLColumnReference) {
            visit((MySQLColumnReference) expr);
        } else if (expr instanceof MySQLUnaryPostfixOperation) {
            visit((MySQLUnaryPostfixOperation) expr);
        } else if (expr instanceof MySQLBinaryLogicalOperation) {
            visit((MySQLBinaryLogicalOperation) expr);
        } else if (expr instanceof MySQLBinaryComparisonOperation) {
            visit((MySQLBinaryComparisonOperation) expr);
        } else if (expr instanceof MySQLCastOperation) {
            visit((MySQLCastOperation) expr);
        } else if (expr instanceof MySQLInOperation) {
            visit((MySQLInOperation) expr);
        } else if (expr instanceof MySQLBinaryOperation) {
            visit((MySQLBinaryOperation) expr);
        } else if (expr instanceof MySQLStringExpression) {
            visit((MySQLStringExpression) expr);
        } else if (expr instanceof MySQLBetweenOperation) {
            visit((MySQLBetweenOperation) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(MySQLExpression expr) {
        MySQLToStringVisitor visitor = new MySQLToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(MySQLExpression expr) {
        return "";
    }

}
