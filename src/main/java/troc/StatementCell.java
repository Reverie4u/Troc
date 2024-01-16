package troc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import troc.mysql.ast.MySQLExpression;
import troc.mysql.ast.MySQLUnaryPostfixOperation;
import troc.mysql.ast.MySQLUnaryPrefixOperation;
import troc.mysql.ast.MySQLUnaryPrefixOperation.MySQLUnaryPrefixOperator;

enum StatementType {
    UNKNOWN,
    SELECT, SELECT_SHARE, SELECT_UPDATE,
    UPDATE, DELETE, INSERT, SET,
    BEGIN, COMMIT, ROLLBACK,
}

@Slf4j
public class StatementCell {
    Transaction tx;
    int statementId;
    String statement;
    StatementType type;
    String wherePrefix = "";
    String whereClause = "";
    String forPostfix = "";
    HashMap<String, String> values = new HashMap<>();
    boolean blocked;
    boolean aborted;
    View view;
    ArrayList<Object> result;
    int newRowId;
    String exceptionMessage = "";
    MySQLExpression predicate;

    public StatementCell(Transaction tx, int statementId) {
        this.tx = tx;
        this.statementId = statementId;
    }

    public StatementCell(Transaction tx, int statementId, String statement) {
        this.tx = tx;
        this.statementId = statementId;
        this.statement = statement.replace(";", "");
        this.type = StatementType.valueOf(this.statement.split(" ")[0]);
        this.parseStatement();
    }

    public StatementCell(Transaction tx, int statementId, String statement, MySQLExpression predicate) {
        this.tx = tx;
        this.statementId = statementId;
        this.statement = statement.replace(";", "");
        this.type = StatementType.valueOf(this.statement.split(" ")[0]);
        log.info("before parse: {}", this.statement);
        this.parseStatement();
        log.info("after parse: {}", this.statement);
        this.predicate = predicate;
    }

    private void parseStatement() {
        int whereIdx, forIdx = -1;
        StatementType realType = type;
        String stmt = this.statement;
        try {
            switch (type) {
                case BEGIN:
                case COMMIT:
                case ROLLBACK:
                    break;
                case SELECT:
                    forIdx = stmt.indexOf("FOR ");
                    if (forIdx == -1) {
                        forIdx = stmt.indexOf("LOCK IN SHARE MODE");
                        if (forIdx == -1) {
                            forPostfix = "";
                        }
                    }
                    if (forIdx != -1) {
                        String postfix = stmt.substring(forIdx);
                        stmt = stmt.substring(0, forIdx - 1);
                        forPostfix = " " + postfix;
                        if (postfix.equals("FOR UPDATE")) {
                            realType = StatementType.SELECT_UPDATE;
                        } else if (postfix.equals("FOR SHARE") || postfix.equals("LOCK IN SHARE MODE")) {
                            realType = StatementType.SELECT_SHARE;
                        } else {
                            throw new RuntimeException("Invalid postfix: " + this.statement);
                        }
                    }
                case UPDATE:
                    int setIdx = stmt.indexOf(" SET ");
                    if (setIdx != -1) {
                        whereIdx = stmt.indexOf(" WHERE ");
                        String setPairsStr;
                        if (whereIdx == -1) {
                            setPairsStr = stmt.substring(setIdx);
                        } else {
                            setPairsStr = stmt.substring(setIdx + 5, whereIdx);
                        }
                        setPairsStr = setPairsStr.replace(" ", "");
                        String[] setPairsList = setPairsStr.split(",");
                        for (String setPair : setPairsList) {
                            int eqIdx = setPair.indexOf("=");
                            String col = setPair.substring(0, eqIdx);
                            String val = setPair.substring(eqIdx + 1);
                            if (val.startsWith("\"") && val.endsWith("\"")) {
                                val = val.substring(1, val.length() - 1);
                            }
                            this.values.put(col, val);
                        }
                    }
                case DELETE:
                    whereIdx = stmt.indexOf("WHERE");
                    if (whereIdx == -1) {
                        wherePrefix = stmt;
                        whereClause = "TRUE";
                    } else {
                        wherePrefix = stmt.substring(0, whereIdx - 1);
                        whereClause = stmt.substring(whereIdx + 6);
                    }
                    this.type = realType;
                    recomputeStatement();
                    break;
                case INSERT:
                    Pattern pattern = Pattern.compile("INTO " + TableTool.TableName
                            + "\\s*\\((.*?)\\) VALUES\\s*\\((.*?)\\)");
                    Matcher matcher = pattern.matcher(this.statement);
                    if (!matcher.find()) {
                        throw new RuntimeException("parse INSERT statement failed");
                    }
                    String[] cols = matcher.group(1).split(",\\s*");
                    String[] vals = matcher.group(2).split(",\\s*");
                    if (cols.length != vals.length) {
                        throw new RuntimeException("Parse insert statement failed: " + this.statement);
                    }
                    for (int i = 0; i < cols.length; i++) {
                        String val = vals[i];
                        if (val.startsWith("\"") && val.endsWith("\"")) {
                            val = val.substring(1, val.length() - 1);
                        }
                        this.values.put(cols[i], val);
                    }
                    break;
                default:
                    throw new RuntimeException("Invalid statement: " + this.statement);
            }
        } catch (Exception e) {
            log.info("Parse statement failed: {}", statement);
            e.printStackTrace();
        }
    }

    public void makeChooseRow(int rowId) {
        String query = null;
        Statement statement;
        ResultSet rs;
        try {
            query = String.format("SELECT * FROM %s WHERE (%s) AND %s = %d",
                    TableTool.TableName, this.whereClause, TableTool.RowIdColName, rowId);
            log.info(query);
            statement = TableTool.conn.createStatement();
            rs = statement.executeQuery(query);
            boolean match = rs.next();
            statement.close();
            rs.close();
            // 如果查询返回了结果，说明当前where子句已经可以选择行ID为rowId的指定行。
            if (match)
                return;
            // 类似于PQS，给Where条件加上IS NULL或者NOT以确保能够查询到rowId的指定行。
            // 不过这里不是约束求解，而是直接查询数据库。
            query = String.format("SELECT (%s) FROM %s WHERE %s = %d",
                    this.whereClause, TableTool.TableName, TableTool.RowIdColName, rowId);
            log.info(query);
            statement = TableTool.conn.createStatement();
            rs = statement.executeQuery(query);
            if (!rs.next()) {
                // 为什么会出现没有结果？
                // 失败是因为当前表中没有数据
                log.info(TableTool.tableToView().toString());
                log.info("Choose row failed, rowId:{}, statement:{}", rowId, this.statement);
                return;
            }
            Object res = rs.getObject(1);
            if (res == null) {
                this.whereClause = "(" + this.whereClause + ") IS NULL";
                this.predicate = new MySQLUnaryPostfixOperation(this.predicate,
                        MySQLUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
            } else {
                this.whereClause = "NOT (" + this.whereClause + ")";
                this.predicate = new MySQLUnaryPrefixOperation(this.predicate, MySQLUnaryPrefixOperator.NOT);
            }
            recomputeStatement();
        } catch (SQLException e) {
            log.info("Execute query failed: {}", query);
            throw new RuntimeException("Execution failed: ", e);
        }
    }

    public void negateCondition() {
        String query = "SELECT (" + whereClause + ") as yes from " + TableTool.TableName + " limit 1";
        TableTool.executeQueryWithCallback(query, (rs) -> {
            try {
                if (!rs.next()) {
                    String res = rs.getString("yes");
                    if (res == null || res.equals("null")) {
                        whereClause = "(" + whereClause + ") IS NULL";
                    } else if (res.equals("0")) {
                        whereClause = "NOT (" + whereClause + ")";
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    public void recomputeStatement() {
        this.statement = wherePrefix + " WHERE " + whereClause + forPostfix;
    }

    public String toString() {
        String res = tx.txId + "-" + statementId;
        if (blocked) {
            res += "(B)";
        }
        if (aborted) {
            res += "(A)";
        }
        return res;
    }

    public boolean equals(StatementCell that) {
        if (that == null) {
            return false;
        }
        return tx.txId == that.tx.txId && statementId == that.statementId;
    }

    public StatementCell copy() {
        StatementCell copy = new StatementCell(tx, statementId);
        copy.statement = statement;
        copy.type = type;
        copy.wherePrefix = wherePrefix;
        copy.whereClause = whereClause;
        copy.forPostfix = forPostfix;
        copy.values = values;
        copy.blocked = false;
        copy.result = null;
        // 共享引用，因为predicate不会再修改了
        copy.predicate = predicate;
        return copy;
    }

    public String getStatement() {
        return statement;
    }
}
