package troc.mysql.visitor;

import java.util.List;
import java.util.stream.Collectors;

import troc.MySQLExpressionBaseVisitor;
import troc.MySQLExpressionParser;
import troc.mysql.MySQLColumn;
import troc.mysql.ast.MySQLBetweenOperation;
import troc.mysql.ast.MySQLCastOperation;
import troc.mysql.ast.MySQLCastOperation.CastType;
import troc.mysql.ast.MySQLColumnReference;
import troc.mysql.ast.MySQLConstant.MySQLIntConstant;
import troc.mysql.ast.MySQLExpression;
import troc.mysql.ast.MySQLInOperation;

public class MySQLExpressionVisitorImpl extends MySQLExpressionBaseVisitor<MySQLExpression> {
    @Override
    public MySQLExpression visitExpression(MySQLExpressionParser.ExpressionContext ctx) {
        if (ctx.literal() != null) {
            // This is a literal
            return visitLiteral(ctx.literal());
        } else if (ctx.BETWEEN() != null) {
            return new MySQLBetweenOperation(visit(ctx.expression(0)), visit(ctx.expression(1)),
                    visit(ctx.expression(2)));
        } else if (ctx.IN() != null) {
            boolean isTrue = ctx.NOT() == null;
            // 获取expressionList并转化成List<MySQLExpression>
            List<MySQLExpression> expressions = ctx.expressionList().expression().stream().map(this::visit)
                    .collect(Collectors.toList());
            return new MySQLInOperation(visit(ctx.expression(0)), expressions, isTrue);
        } else if (ctx.CAST() != null) {
            CastType type = ("SIGNED").equals(ctx.TYPE().getText()) ? CastType.SIGNED : CastType.UNSIGNED;
            return new MySQLCastOperation(visit(ctx.expression(0)), type);
        } else if (ctx.LEFT_BRACKET() != null) {
            return visit(ctx.expression(0));
        } else {
            throw new UnsupportedOperationException("Unsupported expression: " + ctx.getText());
        }
    }

    @Override
    public MySQLExpression visitLiteral(MySQLExpressionParser.LiteralContext ctx) {
        if (ctx.INTEGER_LITERAL() != null) {
            return new MySQLIntConstant(Long.parseLong(ctx.INTEGER_LITERAL().getText()));
        } else if (ctx.SIGNED_INTEGER_LITERAL() != null) {
            return new MySQLIntConstant(Long.parseLong(ctx.SIGNED_INTEGER_LITERAL().getText()));
        } else if (ctx.COLUMN_NAME() != null) {
            MySQLColumn column = new MySQLColumn(null, ctx.COLUMN_NAME().getText(), null, false, false, false, 0);
            return new MySQLColumnReference(column, null);
        } else {
            throw new UnsupportedOperationException("Unsupported literal: " + ctx.getText());
        }
    }
}