package troc.reducer;

import troc.StatementCell;
import troc.TableTool;
import troc.TrocChecker;

public class DTOracleChecker implements OracleChecker {
    @Override
    public boolean hasBug(TestCase tc) {
        TrocChecker checker = new TrocChecker(tc.tx1, tc.tx2);
        // 对表进行处理
        TableTool.executeOnTable("DROP TABLE IF EXISTS " + TableTool.TableName);
        TableTool.executeOnTable(tc.createStmt.getStatement());
        for (StatementCell stmt : tc.prepareTableStmts) {
            TableTool.executeOnTable(stmt.getStatement());
        }
        TableTool.isReducer = true;
        TableTool.preProcessTable();
        boolean res = checker.oracleCheck(tc.submittedOrder);
        TableTool.isReducer = false;
        return !res;
    }

    @Override
    public boolean hasBug(String tc) {
        TestCase testCase = Reducer.parse(tc);
        // 给testCase的两个事务设置连接
        testCase.tx1.setConn(TableTool.genConnection());
        testCase.tx1.setRefConn(TableTool.genAnotherConnection());
        testCase.tx2.setConn(TableTool.genConnection());
        testCase.tx2.setRefConn(TableTool.genAnotherConnection());
        return hasBug(testCase);
    }
}
