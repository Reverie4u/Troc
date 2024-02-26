package troc.reducer;

import java.util.ArrayList;
import java.util.List;

import troc.StatementCell;
import troc.Transaction;

public class TestCase {
    StatementCell createStmt;
    List<StatementCell> prepareTableStmts;
    Transaction tx1;
    Transaction tx2;
    List<StatementCell> submittedOrder;

    public TestCase() {
        prepareTableStmts = new ArrayList<>();
        submittedOrder = new ArrayList<>();
    }

    // 需要重写toString方法
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(createStmt.getStatement());
        return sb.toString();
    }
}
