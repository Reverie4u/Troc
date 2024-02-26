package troc.reducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import troc.IsolationLevel;
import troc.StatementCell;
import troc.StatementType;
import troc.Transaction;

public class Reducer {
    // 全局只有一个Reducer
    OrderSelector<StatementType> stmtDelOrderSelector;
    Map<StatementType, ArrayList<StatementCell>> stmtTypeMap;
    static int maxReduceCount = 5;

    public Reducer() {
        stmtTypeMap = new HashMap<>();
        List<StatementType> candidates = new ArrayList<>();
        StatementType[] types = new StatementType[] { StatementType.BEGIN, StatementType.COMMIT, StatementType.ROLLBACK,
                StatementType.SELECT, StatementType.UPDATE, StatementType.INSERT, StatementType.DELETE,
                StatementType.CREATE_INDEX, StatementType.SELECT_SHARE,
                StatementType.SELECT_UPDATE };
        for (StatementType type : types) {
            stmtTypeMap.put(type, new ArrayList<>());
            candidates.add(type);
        }
        stmtDelOrderSelector = new RandomOrderSelector<>(candidates);
    }

    private ArrayList<StatementCell> stmtListClone(List<StatementCell> stmts) {
        ArrayList<StatementCell> copied = new ArrayList<>();
        for (StatementCell stmt : stmts) {
            copied.add(stmt.copy());
        }
        return copied;
    }

    private TestCase testCaseClone(TestCase tc) {
        TestCase clonedTestCase = new TestCase();
        clonedTestCase.createStmt = tc.createStmt.copy();
        clonedTestCase.prepareTableStmts = stmtListClone(tc.prepareTableStmts);
        clonedTestCase.tx1 = new Transaction(1, tc.tx1.getIsolationlevel(), tc.tx1.getConn(), tc.tx1.getRefConn());
        clonedTestCase.tx1.setStatements(stmtListClone(tc.tx1.getStatements()));
        clonedTestCase.tx2 = new Transaction(2, tc.tx2.getIsolationlevel(), tc.tx2.getConn(), tc.tx2.getRefConn());
        clonedTestCase.tx2.setStatements(stmtListClone(tc.tx2.getStatements()));
        clonedTestCase.submittedOrder = stmtListClone(tc.submittedOrder);
        return clonedTestCase;
    }

    public String reduce(String tc) {
        TestCase testCase = parse(tc);
        // 将语句按照类型划分
        for (StatementCell stmt : testCase.prepareTableStmts) {
            stmtTypeMap.get(stmt.getType()).add(stmt);
        }
        for (StatementCell stmt : testCase.tx1.getStatements()) {
            stmtTypeMap.get(stmt.getType()).add(stmt);
        }
        for (StatementCell stmt : testCase.tx2.getStatements()) {
            stmtTypeMap.get(stmt.getType()).add(stmt);
        }
        OracleChecker oracleChecker = new MTOracleChecker();
        // 选择一个语句类型
        for (int i = 0; i < maxReduceCount; i++) {
            // 首先克隆一份testcase
            TestCase clonedTestCase = testCaseClone(testCase);
            // 然后按照顺序删除语句
            if (!oracleChecker.hasBug(clonedTestCase)) {
                // 删除后仍能复现bug则更新测试用例
                testCase = clonedTestCase;
            }
        }
        // 将测试用例转换为字符串输出
        return testCase.toString();
    }

    /*
     * 从Scanner中读取事务
     */
    static Transaction readTransaction(Scanner input, int txId) {
        Transaction tx = new Transaction(txId);
        String isolationAlias = input.nextLine();
        tx.setIsolationlevel(IsolationLevel.getFromAlias(isolationAlias));
        String sql;
        int cnt = 0;
        do {
            if (!input.hasNext())
                break;
            sql = input.nextLine();
            if (sql.equals("") || sql.equals("END"))
                break;
            tx.getStatements().add(new StatementCell(tx, cnt++, sql));
        } while (true);
        return tx;
    }

    /*
     * 从Scanner中读取提交顺序
     */
    static String readSchedule(Scanner input) {
        do {
            if (!input.hasNext())
                break;
            String scheduleStr = input.nextLine();
            if (scheduleStr.equals(""))
                continue;
            if (scheduleStr.equals("END"))
                break;
            return scheduleStr;
        } while (true);
        return "";
    }

    /*
     * 解析提交顺序是否合法
     */
    public ArrayList<StatementCell> parseSchedule(Transaction tx1, Transaction tx2, String scheduleStr) {
        String[] schedule = scheduleStr.split("-");
        int len1 = tx1.getStatements().size();
        int len2 = tx2.getStatements().size();
        if (schedule.length != len1 + len2) {
            throw new RuntimeException("Invalid Schedule");
        }
        ArrayList<StatementCell> submittedOrder = new ArrayList<>();
        int idx1 = 0, idx2 = 0;
        for (String txId : schedule) {
            if (txId.equals("1")) {
                submittedOrder.add(tx1.getStatements().get(idx1++));
            } else if (txId.equals("2")) {
                submittedOrder.add(tx2.getStatements().get(idx2++));
            } else {
                throw new RuntimeException("Invalid Schedule");
            }
        }
        return submittedOrder;
    }

    /*
     * 解析测试用例
     */
    private TestCase parse(String tc) {
        // 从字符串读取事务
        Scanner input = new Scanner(tc);
        // 读取建表语句
        String sql;
        TestCase testCase = new TestCase();
        sql = input.nextLine();
        testCase.createStmt = new StatementCell(null, -1, sql);
        do {
            sql = input.nextLine();
            if (sql.equals(""))
                break;
            testCase.prepareTableStmts.add(new StatementCell(null, -1, sql));
        } while (true);
        // 读取其他语句
        testCase.tx1 = readTransaction(input, 1);
        testCase.tx2 = readTransaction(input, 2);
        // 读取提交顺序
        String scheduleStr = readSchedule(input);
        testCase.submittedOrder = parseSchedule(testCase.tx1, testCase.tx2, scheduleStr);
        input.close();
        return testCase;
    }
}
