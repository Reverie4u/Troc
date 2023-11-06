package troc;

import troc.common.Table;
import com.beust.jcommander.JCommander;
import lombok.extern.slf4j.Slf4j;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Options options = new Options();
        JCommander jCmd = new JCommander();
        jCmd.addObject(options);
        jCmd.parse(args);
        verifyOptions(options);
        log.info(String.format("Run tests for %s in [DB %s]-[Table %s] on [%s:%d]",
                options.getDBMS(), options.getDbName(), options.getTableName(), options.getHost(), options.getPort()));

        txnTesting(options);
        TableTool.cleanTrocTables();
    }

    private static void txnTesting(Options options) {
        TableTool.initialize(options);
        Transaction tx1, tx2;
        if (options.isSetCase()) {
            // 从文件或命令行读取事务
            Scanner scanner;
            if (options.getCaseFile().equals("")) {
                log.info("Read database and transactions from command line");
                scanner = new Scanner(System.in);
            } else {
                try {
                    File caseFile = new File(options.getCaseFile());
                    scanner = new Scanner(caseFile);
                    log.info("Read database and transactions from file: {}", options.getCaseFile());
                } catch (FileNotFoundException e) {
                    throw new RuntimeException("Read case from file failed: ", e);
                }
            }
            // 执行文件中或命令行输入的建表语句
            TableTool.prepareTableFromScanner(scanner);
            // 对表进行预处理
            TableTool.preProcessTable();
            log.info("Initial table:\n{}", TableTool.tableToView());
            // 读取两个事务
            tx1 = TableTool.readTransactionFromScanner(scanner, 1);
            tx2 = TableTool.readTransactionFromScanner(scanner, 2);
            // 读取提交顺序
            String scheduleStr = TableTool.readScheduleFromScanner(scanner);
            scanner.close();
            log.info("Read transactions from file:\n{}{}", tx1, tx2);
            TableTool.txPair++;
            TrocChecker checker = new TrocChecker(tx1, tx2);
            if (!scheduleStr.equals("")) {
                log.info("Get schedule from file: {}", scheduleStr);
                // 根据读取的提交顺序进行check
                checker.checkSchedule(scheduleStr);
            } else {
                checker.checkAll();
            }
        } else {
            while (true) {
                // 循环fuzzing
                log.info("Create new table.");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}
                Table table = TableTool.dbms.buildTable(options.getTableName());
                table.initialize();
                log.info(table.getCreateTableSql());
                TableTool.preProcessTable();
                TableTool.bugReport.setCreateTableSQL(table.getCreateTableSql());
                TableTool.bugReport.setInitializeStatements(table.getInitializeStatements());
                TableTool.bugReport.setInitialTable(TableTool.tableToView().toString());
                log.info("Initial table:\n{}", TableTool.tableToView());
                TableTool.txPairHasConflict = false;
                for (int _i = 0; _i < 5; _i++) {
                    log.info("Generate new transaction pair.");
                    TableTool.txPair++;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {}
                    // 恢复原始table
                    TableTool.recoverOriginalTable();
                    // 生成两个事务
                    tx1 = table.genTransaction(1);
                    tx2 = table.genTransaction(2);
                    // 手动构建冲突
                    TableTool.makeConflict(tx1, tx2);
                    TableTool.bugReport.setTx1(tx1);
                    TableTool.bugReport.setTx2(tx2);
                    log.info("Transaction 1:\n{}", tx1);
                    log.info("Transaction 2:\n{}", tx2);
                    TrocChecker checker = new TrocChecker(tx1, tx2);
                    // 随机生成提交顺序
                    checker.checkRandom();
                }
                if(TableTool.txPairHasConflict){
                    TableTool.conflictTxPair++;
                }
                log.info("txPair:{}, conflictTxPair:{}, allCase:{}, conflictCase:{}", 
                        TableTool.txPair, TableTool.conflictTxPair, TableTool.allCase, TableTool.conflictCase);
            }
        }
    }

    private static void verifyOptions(Options options) {
        options.setDBMS(options.getDBMS().toUpperCase());
        if(Arrays.stream(DBMS.values()).map(DBMS::name).noneMatch(options.getDBMS()::equals)) {
            throw new RuntimeException("Unknown DBMS: " + options.getDBMS());
        }
    }
}
