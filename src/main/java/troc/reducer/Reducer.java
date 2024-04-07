package troc.reducer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import lombok.val;
import lombok.extern.slf4j.Slf4j;
import troc.IsolationLevel;
import troc.Randomly;
import troc.StatementCell;
import troc.StatementType;
import troc.TableTool;
import troc.Transaction;

@Slf4j
public class Reducer {
    // 全局只有一个Reducer
    OrderSelector<StatementType> stmtDelOrderSelector;
    // 第一层简化的map
    Map<StatementType, ArrayList<StatementCell>> stmtTypeMap;
    // 第一层简化失败的语句map
    Map<StatementType, ArrayList<StatementCell>> stmtTypeFailMap;
    // 最大简化次数
    int maxReduceCount = 0;
    int allReduceCount = 0;
    int vaildReduceCount = 0;

    public int getVaildReduceCount() {
        return vaildReduceCount;
    }

    public int getAllReduceCount() {
        return allReduceCount;
    }

    public Reducer(int selectorType) {
        maxReduceCount = TableTool.maxReduceCount;
        stmtTypeMap = new HashMap<>();
        stmtTypeFailMap = new HashMap<>();
        List<StatementType> candidates = new ArrayList<>();
        StatementType[] types = new StatementType[] {
                StatementType.SELECT, StatementType.UPDATE, StatementType.INSERT, StatementType.DELETE,
                StatementType.CREATE_INDEX, StatementType.SELECT_SHARE,
                StatementType.SELECT_UPDATE };
        for (StatementType type : types) {
            stmtTypeMap.put(type, new ArrayList<>());
            stmtTypeFailMap.put(type, new ArrayList<>());
            candidates.add(type);
        }
        switch (selectorType) {
            case 0:
                stmtDelOrderSelector = new RandomOrderSelector<>(candidates);
                break;
            case 1:
                stmtDelOrderSelector = new ProbabilityTableBasedOrderSelector<>(candidates);
                break;
            case 2:
                stmtDelOrderSelector = new EpsilonGreedyOrderSelector<>(candidates);
                break;
            default:
                break;
        }
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
        // *删除语句层*
        // stmtTypeMap和stmtTypeFailMap需要清空
        for (Map.Entry<StatementType, ArrayList<StatementCell>> entry : stmtTypeMap.entrySet()) {
            entry.getValue().clear();
        }
        for (Map.Entry<StatementType, ArrayList<StatementCell>> entry : stmtTypeFailMap.entrySet()) {
            entry.getValue().clear();
        }
        TestCase testCase = parse(tc);
        // 将语句按照类型划分
        for (StatementCell stmt : testCase.prepareTableStmts) {
            stmtTypeMap.get(stmt.getType()).add(stmt);
        }
        for (StatementCell stmt : testCase.tx1.getStatements()) {
            if (stmt.getType() != StatementType.BEGIN && stmt.getType() != StatementType.COMMIT
                    && stmt.getType() != StatementType.ROLLBACK) {
                stmtTypeMap.get(stmt.getType()).add(stmt);
            }
        }
        for (StatementCell stmt : testCase.tx2.getStatements()) {
            if (stmt.getType() != StatementType.BEGIN && stmt.getType() != StatementType.COMMIT
                    && stmt.getType() != StatementType.ROLLBACK) {
                stmtTypeMap.get(stmt.getType()).add(stmt);
            }
        }
        OracleChecker oracleChecker;
        if (TableTool.oracle.equals("MT")) {
            oracleChecker = new MTOracleChecker();
        } else {
            oracleChecker = new DTOracleChecker();
        }
          
        // 选择一个语句类型
        for (int i = 0; i < maxReduceCount; i++) {
            // 首先克隆一份testcase
            TestCase clonedTestCase = testCaseClone(testCase);
            StatementCell delStmt = deleteStatement(clonedTestCase);
            if (delStmt == null) {
                break;
            }
            allReduceCount++;
            if (oracleChecker.hasBug(clonedTestCase.toString())) {
                log.info("Statement [{}] del success", delStmt.toString());
                // 删除后仍能复现bug则更新测试用例
                testCase = clonedTestCase;
                vaildReduceCount++;
                stmtDelOrderSelector.updateWeight(delStmt.getType(), true);
            } else {
                log.info("Statement [{}] del failed", delStmt.toString());
                stmtTypeFailMap.get(delStmt.getType()).add(delStmt);
                stmtDelOrderSelector.updateWeight(delStmt.getType(), false);
            }
        }
        // *语句简化层*
        // 简化表定义     
        testCase = simplifyTable(testCase, oracleChecker);
        // 删除插入列 
        testCase = delInsertCol(testCase, oracleChecker);
        // 删除更新列
        testCase = delUpdateCol(testCase, oracleChecker);
        // 删除表达式
        testCase = delWhereClause(testCase,oracleChecker);
        // 将测试用例转换为字符串输出
        String res = testCase.toString();
        System.out.println(res);
        return res;
    }
    private TestCase delWhereClause(TestCase testCase, OracleChecker oracleChecker){
        // 函数只有for()里的集合不一样
        testCase = delWhereClauseTx1(testCase, oracleChecker);
        testCase = delWhereClauseTx2(testCase, oracleChecker);
        return testCase;
    }
    private TestCase delInsertCol(TestCase testCase, OracleChecker oracleChecker){
        // 函数只有for()里的集合不一样
        testCase = delInsertColPre(testCase, oracleChecker);
        testCase = delInsertColTx1(testCase, oracleChecker);
        testCase = delInsertColTx2(testCase, oracleChecker);
        return testCase;
    }
    private TestCase delUpdateCol(TestCase testCase, OracleChecker oracleChecker){
        // 两个函数只有for()里的集合不一样
        testCase = delUpdateColTx1(testCase, oracleChecker);
        testCase = delUpdateColTx2(testCase, oracleChecker);
        return testCase;
    }
    private TestCase simplifyTable(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        String createSQL = clonedTestCase.createStmt.getStatement();
        // 找到")"位置然后将后面的表定义替换掉
        int rightBracketIdx = createSQL.lastIndexOf(")");
        clonedTestCase.createStmt.setStmt(createSQL.substring(0, rightBracketIdx)+");");
     //   System.out.println(clonedTestCase.createStmt.getStatement());

        if (oracleChecker.hasBug(clonedTestCase.toString())) {
            log.info("createSQL simpilify success");
            // 删除后仍能复现bug则更新测试用例
            testCase = testCaseClone(clonedTestCase); 
        } else {
            log.info("createSQL simpilify failed");
        }
        return testCase;
    }
    private TestCase delWhereClauseTx1(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for(StatementCell whereCell : clonedTestCase.tx1.getStatements()){
            if(whereCell.getWhereClause() != ""){
              //  StatementCell whereCellCopy = whereCell.copy();
                StringBuffer delWhereClause = new StringBuffer();
                delWhereClause.append(whereCell.getWherePrefix());
                delWhereClause.append(whereCell.getForPostFix());
                // 删除事务Tx1语句中where部分以及Predicate谓词
                whereCell.setStmt(delWhereClause.toString());
                whereCell.setPredicate(null);
                whereCell.setWhereClause("");
                
                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("whereSQL [{}] simpilify success",whereCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("whereSQL [{}] simpilify failed",whereCell.getStatement().toString());
                }
            }
        }
        return testCase;
    }
    private TestCase delWhereClauseTx2(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for(StatementCell whereCell : clonedTestCase.tx2.getStatements()){
            if(whereCell.getWhereClause() != ""){
              //  StatementCell whereCellCopy = whereCell.copy();
                StringBuffer delWhereClause = new StringBuffer();
                delWhereClause.append(whereCell.getWherePrefix());
                delWhereClause.append(whereCell.getForPostFix());
                // 删除事务Tx2语句中where部分以及Predicate谓词
                whereCell.setStmt(delWhereClause.toString());
                whereCell.setPredicate(null);
                whereCell.setWhereClause("");
                
                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("whereSQL [{}] simpilify success",whereCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("whereSQL [{}] simpilify failed",whereCell.getStatement().toString());
                }
            }
        }
        return testCase;
    }
    private TestCase delInsertColPre(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for (StatementCell insertCell : clonedTestCase.prepareTableStmts){
            if (insertCell.getType() == StatementType.INSERT){
                StatementCell insertCellCopy = insertCell.copy();
                String insertStmt = insertCellCopy.getStatement();
                // 取得列名括号对和值括号对的位置
                int columnLeftBracket = insertStmt.indexOf("(");
                int columnRightBracket = insertStmt.indexOf(")");
                int valueLeftBracket = insertStmt.lastIndexOf("(");
                int valueRightBracket = insertStmt.lastIndexOf(")");
                // 得到插入列
                String columnStr = insertStmt.substring(columnLeftBracket+1, columnRightBracket);
                String[] columnWords = columnStr.split(",");
                // 得到插入值
                String valueStr = insertStmt.substring(valueLeftBracket+1, valueRightBracket);
                String[] valueWords = valueStr.split(",");
                // 如果只有一列就不删了
                if(columnWords.length == 1) continue;
                // 剩下的都是至少两列插入，随机选择一列删除
                int idx = Randomly.getNextInt(0,columnWords.length-1);
                StringBuffer newColumnStr = new StringBuffer();
                StringBuffer newValueStr  = new StringBuffer();
                for(int i=0, cntVal=0, cntCol=0, cnt=0; i<columnWords.length; i++){
                    // 不补null的
                    if(i==idx && idx ==0) continue;
                    else if(i!=idx && idx!=0){
                        if(i==0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                        }
                    }else if(i!=idx && idx==0){
                        if(cnt==0){
                            newValueStr.append(valueWords[i].substring(1));
                            newColumnStr.append(columnWords[i].substring(1));
                            cnt++;
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cnt++;
                        }
                    }
                    /*  补null的
                    if(i==idx && idx==0){
                        newValueStr.append("null");
                        cntVal++;
                    }
                    else if(i==idx && idx!=0){
                        newValueStr.append(", null");
                        cntVal++;
                    }
                    else if(i!=idx && idx !=0){
                        if(cntCol==0 && cntVal == 0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }
                    else if(i!=idx && idx == 0){
                        if(cntCol==0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(columnWords[i].substring(1));
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }*/
                }
                StringBuffer newInsertStmt = new StringBuffer();
                // 将去掉某一列后的column列和value列与原先句子进行拼接 Pre里INSERT句子最后加个";"，事务里不需要
                newInsertStmt.append(insertStmt.substring(0, columnLeftBracket+1));
                newInsertStmt.append(newColumnStr.toString());
                newInsertStmt.append(insertStmt.substring(columnRightBracket, valueLeftBracket+1));                                          
                newInsertStmt.append(newValueStr.toString());
                newInsertStmt.append(insertStmt.substring(valueRightBracket)+";");
                
                insertCell.setStmt(newInsertStmt.toString());

                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("insertSQL [{}] simpilify success",insertCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("insertSQL [{}] simpilify failed",insertCell.getStatement().toString());
                }
            }
            
        }
        return testCase;
    }
    private TestCase delInsertColTx1(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for (StatementCell insertCell : clonedTestCase.tx1.getStatements()){
            if (insertCell.getType() == StatementType.INSERT){
                StatementCell insertCellCopy = insertCell.copy();
                String insertStmt = insertCellCopy.getStatement();
                // 取得列名括号对和值括号对的位置
                int columnLeftBracket = insertStmt.indexOf("(");
                int columnRightBracket = insertStmt.indexOf(")");
                int valueLeftBracket = insertStmt.lastIndexOf("(");
                int valueRightBracket = insertStmt.lastIndexOf(")");
                // 得到插入列
                String columnStr = insertStmt.substring(columnLeftBracket+1, columnRightBracket);
                String[] columnWords = columnStr.split(",");
                // 得到插入值
                String valueStr = insertStmt.substring(valueLeftBracket+1, valueRightBracket);
                String[] valueWords = valueStr.split(",");
                // 如果只有一列就不删了
                if(columnWords.length == 1) continue;
                // 剩下的都是至少两列插入，随机选择一列删除
                int idx = Randomly.getNextInt(0,columnWords.length-1);
                StringBuffer newColumnStr = new StringBuffer();
                StringBuffer newValueStr  = new StringBuffer();
                for(int i=0, cntVal=0, cntCol=0, cnt=0; i<columnWords.length; i++){
                    // 不补null的
                    if(i==idx && idx ==0) continue;
                    else if(i!=idx && idx!=0){
                        if(i==0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                        }
                    }else if(i!=idx && idx==0){
                        if(cnt==0){
                            newValueStr.append(valueWords[i].substring(1));
                            newColumnStr.append(columnWords[i].substring(1));
                            cnt++;
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cnt++;
                        }
                    }
                    /*  补null的
                    if(i==idx && idx==0){
                        newValueStr.append("null");
                        cntVal++;
                    }
                    else if(i==idx && idx!=0){
                        newValueStr.append(", null");
                        cntVal++;
                    }
                    else if(i!=idx && idx !=0){
                        if(cntCol==0 && cntVal == 0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }
                    else if(i!=idx && idx == 0){
                        if(cntCol==0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(columnWords[i].substring(1));
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }*/
                }
                StringBuffer newInsertStmt = new StringBuffer();
                // 将去掉某一列后的column列和value列与原先句子进行拼接 Pre里INSERT句子最后加个";"，事务里不需要
                newInsertStmt.append(insertStmt.substring(0, columnLeftBracket+1));
                newInsertStmt.append(newColumnStr.toString());
                newInsertStmt.append(insertStmt.substring(columnRightBracket, valueLeftBracket+1));                                          
                newInsertStmt.append(newValueStr.toString());
                newInsertStmt.append(insertStmt.substring(valueRightBracket));
                
                insertCell.setStmt(newInsertStmt.toString());

                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("insertSQL [{}] simpilify success",insertCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("insertSQL [{}] simpilify failed",insertCell.getStatement().toString());
                }
            }
            
        }
        return testCase;
    }
    private TestCase delInsertColTx2(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for (StatementCell insertCell : clonedTestCase.tx2.getStatements()){
            if (insertCell.getType() == StatementType.INSERT){
                StatementCell insertCellCopy = insertCell.copy();
                String insertStmt = insertCellCopy.getStatement();
                // 取得列名括号对和值括号对的位置
                int columnLeftBracket = insertStmt.indexOf("(");
                int columnRightBracket = insertStmt.indexOf(")");
                int valueLeftBracket = insertStmt.lastIndexOf("(");
                int valueRightBracket = insertStmt.lastIndexOf(")");
                // 得到插入列
                String columnStr = insertStmt.substring(columnLeftBracket+1, columnRightBracket);
                String[] columnWords = columnStr.split(",");
                // 得到插入值
                String valueStr = insertStmt.substring(valueLeftBracket+1, valueRightBracket);
                String[] valueWords = valueStr.split(",");
                // 如果只有一列就不删了
                if(columnWords.length == 1) continue;
                // 剩下的都是至少两列插入，随机选择一列删除
                int idx = Randomly.getNextInt(0,columnWords.length-1);
                StringBuffer newColumnStr = new StringBuffer();
                StringBuffer newValueStr  = new StringBuffer();
                for(int i=0, cntVal=0, cntCol=0, cnt=0; i<columnWords.length; i++){
                    // 不补null的
                    if(i==idx && idx ==0) continue;
                    else if(i!=idx && idx!=0){
                        if(i==0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                        }
                    }else if(i!=idx && idx==0){
                        if(cnt==0){
                            newValueStr.append(valueWords[i].substring(1));
                            newColumnStr.append(columnWords[i].substring(1));
                            cnt++;
                        }
                        else{
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cnt++;
                        }
                    }
                    /*  补null的
                    if(i==idx && idx==0){
                        newValueStr.append("null");
                        cntVal++;
                    }
                    else if(i==idx && idx!=0){
                        newValueStr.append(", null");
                        cntVal++;
                    }
                    else if(i!=idx && idx !=0){
                        if(cntCol==0 && cntVal == 0){
                            newValueStr.append(valueWords[i]);
                            newColumnStr.append(columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }
                    else if(i!=idx && idx == 0){
                        if(cntCol==0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(columnWords[i].substring(1));
                            cntVal++;
                            cntCol++;
                        }
                        else if(cntCol!=0){
                            newValueStr.append(","+valueWords[i]);
                            newColumnStr.append(","+columnWords[i]);
                            cntVal++;
                            cntCol++;
                        }
                    }*/
                }
                // 将去掉某一列后的column列和value列与原先句子进行拼接 Pre里INSERT句子最后加个";"，事务里不需要
                StringBuffer newInsertStmt = new StringBuffer();
                newInsertStmt.append(insertStmt.substring(0, columnLeftBracket+1));
                newInsertStmt.append(newColumnStr.toString());
                newInsertStmt.append(insertStmt.substring(columnRightBracket, valueLeftBracket+1));                                          
                newInsertStmt.append(newValueStr.toString());
                newInsertStmt.append(insertStmt.substring(valueRightBracket));
                
                insertCell.setStmt(newInsertStmt.toString());

                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("insertSQL [{}] simpilify success",insertCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("insertSQL [{}] simpilify failed",insertCell.getStatement().toString());
                }
            }
            
        }
        return testCase;
    }
    private TestCase delUpdateColTx1(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for (StatementCell upCell : clonedTestCase.tx1.getStatements()) {
            if (upCell.getType() == StatementType.UPDATE) {
                StatementCell cloneUpCell = upCell.copy();
                String upStmt = cloneUpCell.getStatement();
                // 将SET和WHERE转变成“，”，这样的话以第一个和最后一个“，”就会包含更新列项 将其subString提取出来
                String replacedUpStmt = upStmt.replace("SET", "WHERE");
                String finalReplacedUpStmt = replacedUpStmt.replace("WHERE",",");
               // System.out.println(finalReplacedUpStmt);
                int firstCommaIdx = finalReplacedUpStmt.indexOf(",");
                int lastCommaIdx = finalReplacedUpStmt.lastIndexOf(",");
                String updateStr = finalReplacedUpStmt.substring(firstCommaIdx+1, lastCommaIdx-1);
                // 获取更新列项
                String[] updateWords = updateStr.split(",");
                // 如果只有一个则不进行简化
                if(updateWords.length==1) continue;
                int idx = Randomly.getNextInt(0, updateWords.length-1);
                // 删除并重新拼接
                StringBuffer updateStmt = new StringBuffer();
                for(int i=0, cnt=0; i<updateWords.length;i++){
                    if(i==idx) continue;
                    else{
                        if(cnt==0){
                            updateStmt.append(updateWords[i]);
                            cnt++;
                        }else{
                            updateStmt.append(","+updateWords[i]);
                        }
                    }
                }
                updateStmt.append(" ");
                StringBuffer replacedUpdateStr = new StringBuffer();
                StringBuffer replacedUpdateStrPrefix = new StringBuffer();
    
                replacedUpdateStr.append(finalReplacedUpStmt);
                replacedUpdateStr.replace(firstCommaIdx+1, lastCommaIdx, updateStmt.toString());
               
                replacedUpdateStr.insert(replacedUpdateStr.toString().indexOf(",")+1, "SET");
                replacedUpdateStrPrefix.append(replacedUpdateStr.toString());

                replacedUpdateStr.insert(replacedUpdateStr.toString().lastIndexOf(",")+1,"WHERE");
                replacedUpdateStr.deleteCharAt(replacedUpdateStr.toString().indexOf(","));
                replacedUpdateStrPrefix.deleteCharAt(replacedUpdateStrPrefix.toString().indexOf(","));
                replacedUpdateStr.deleteCharAt(replacedUpdateStr.toString().lastIndexOf(","));

                upCell.setStmt(replacedUpdateStr.toString());
                upCell.setWherePrefix(replacedUpdateStrPrefix.substring(0, replacedUpdateStrPrefix.toString().lastIndexOf(",")-1));
                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("updateSQL [{}] simpilify success",upCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = testCaseClone(clonedTestCase); 
                } else {
                    log.info("updateSQL [{}] simpilify failed",upCell.getStatement().toString());
                }
            }
        }
        return testCase;
    }
    private TestCase delUpdateColTx2(TestCase testCase, OracleChecker oracleChecker){
        TestCase clonedTestCase = testCaseClone(testCase);
        for (StatementCell upCell : clonedTestCase.tx2.getStatements()) {
            if (upCell.getType() == StatementType.UPDATE) {
                StatementCell cloneUpCell = upCell.copy();
                String upStmt = cloneUpCell.getStatement();
                // 将SET和WHERE转变成“，”，这样的话以第一个和最后一个“，”就会包含更新列项 将其subString提取出来
                String replacedUpStmt = upStmt.replace("SET", "WHERE");
                String finalReplacedUpStmt = replacedUpStmt.replace("WHERE",",");
               // System.out.println(finalReplacedUpStmt);
                int firstCommaIdx = finalReplacedUpStmt.indexOf(",");
                int lastCommaIdx = finalReplacedUpStmt.lastIndexOf(",");
                String updateStr = finalReplacedUpStmt.substring(firstCommaIdx+1, lastCommaIdx-1);
                // 获取更新列项
                String[] updateWords = updateStr.split(",");
                // 如果只有一个则不进行简化
                if(updateWords.length==1) continue;
                int idx = Randomly.getNextInt(0, updateWords.length-1);
                // 删除并重新拼接
                StringBuffer updateStmt = new StringBuffer();
                for(int i=0, cnt=0; i<updateWords.length;i++){
                    if(i==idx) continue;
                    else{
                        if(cnt==0){
                            updateStmt.append(updateWords[i]);
                            cnt++;
                        }else{
                            updateStmt.append(","+updateWords[i]);
                        }
                    }
                }
                updateStmt.append(" ");
                StringBuffer replacedUpdateStr = new StringBuffer();
                StringBuffer replacedUpdateStrPrefix = new StringBuffer();
                replacedUpdateStr.append(finalReplacedUpStmt);
                replacedUpdateStr.replace(firstCommaIdx+1, lastCommaIdx, updateStmt.toString());
               
                replacedUpdateStr.insert(replacedUpdateStr.toString().indexOf(",")+1, "SET");
                replacedUpdateStrPrefix.append(replacedUpdateStr.toString());

                replacedUpdateStr.insert(replacedUpdateStr.toString().lastIndexOf(",")+1,"WHERE");
                replacedUpdateStr.deleteCharAt(replacedUpdateStr.toString().indexOf(","));
                replacedUpdateStrPrefix.deleteCharAt(replacedUpdateStrPrefix.toString().indexOf(","));
                replacedUpdateStr.deleteCharAt(replacedUpdateStr.toString().lastIndexOf(","));

                upCell.setStmt(replacedUpdateStr.toString());
                upCell.setWherePrefix(replacedUpdateStrPrefix.substring(0, replacedUpdateStrPrefix.toString().lastIndexOf(",")-1));
                if (oracleChecker.hasBug(clonedTestCase.toString())) {
                    log.info("updateSQL [{}] simpilify success",upCell.getStatement().toString());
                    // 删除后仍能复现bug则更新测试用例
                    testCase = clonedTestCase; 
                } else {
                    log.info("updateSQL [{}] simpilify failed",upCell.getStatement().toString());
                }
            }
        }
        return testCase;
    }
    private StatementCell deleteStatement(TestCase testCase) {
        List<StatementType> excludedTypes = new ArrayList<>();
        // 遍历stmtTypeMap，将列表为空的类型加入excludedTypes
        for (Map.Entry<StatementType, ArrayList<StatementCell>> entry : stmtTypeMap.entrySet()) {
            if (entry.getValue().isEmpty()) {
                excludedTypes.add(entry.getKey());
            }
        }
        StatementType type = stmtDelOrderSelector.selectNext(excludedTypes);
        if (type == null) {
            return null;
        }
        ArrayList<StatementCell> stmts = stmtTypeMap.get(type);
        // System.out.println("待删除语句列表：" + stmts);
        // 随机抽取一个语句删除
        int idx = (int) (Math.random() * stmts.size());
        int txId = stmts.get(idx).getTx().getTxId();
        // 待删除语句
        if (txId == 1) {
            testCase.tx1.getStatements().remove(stmts.get(idx));
        } else if (txId == 2) {
            testCase.tx2.getStatements().remove(stmts.get(idx));
        } else {
            testCase.prepareTableStmts.remove(stmts.get(idx));
        }
        // 更新submittedOrder
        testCase.submittedOrder.remove(stmts.get(idx));
        // TODO: 目前不管本次删除是否合理，都将该语句从stmtTypeMap中删除，后续可以考虑删除失败的情况只对语句做标记。
        StatementCell delStmt = stmts.remove(idx);
        return delStmt;
    }

    /*
     * 从Scanner中读取事务
     */
    public static Transaction readTransaction(Scanner input, int txId) {
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
    public static String readSchedule(Scanner input) {
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
    public static ArrayList<StatementCell> parseSchedule(Transaction tx1, Transaction tx2, String scheduleStr) {
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
    public static TestCase parse(String tc) {
        // 从字符串读取事务
        Scanner input = new Scanner(tc);
        // 读取建表语句
        String sql;
        TestCase testCase = new TestCase();
        sql = input.nextLine();
        testCase.createStmt = new StatementCell(null, -1, sql);
        int i = 0;
        do {
            sql = input.nextLine();
            if (sql.equals(""))
                break;
            testCase.prepareTableStmts.add(new StatementCell(new Transaction(0), i, sql));
            i++;
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
