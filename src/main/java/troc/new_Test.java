package troc;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

import com.beust.jcommander.JCommander;

import lombok.extern.slf4j.Slf4j;
import troc.common.Table;

@Slf4j
public class new_Test {
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
    
       
            log.info("Create new table.");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            Table table = TableTool.dbms.buildTable(options.getTableName());
            table.initialize();

            log.info(table.getCreateTableSql());
            log.info("InitializeStatements: {}", table.getInitializeStatements());
            // 这个地方已经创建好表了，并填充数据了
            TableTool.preProcessTable();
            TableTool.bugReport.setCreateTableSQL(table.getCreateTableSql());
            TableTool.bugReport.setInitializeStatements(table.getInitializeStatements());
            TableTool.bugReport.setInitialTable(TableTool.tableToView().toString());
            log.info("Initial table:\n{}", TableTool.tableToView());
          
                log.info("Generate new transaction pair.");
                TableTool.txPairHasConflict = false;
                TableTool.txPair++;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                // 恢复原始table
                TableTool.recoverOriginalTable();
                log.info("Current table(1):\n{}", TableTool.tableToView());
                // 生成两个事务
                tx1 = table.genTransaction(1);
                tx2 = table.genTransaction(2);
                TableTool.recoverOriginalTable();
                log.info("Current table(2):\n{}", TableTool.tableToView());
                // 手动构建冲突
                TableTool.makeConflict(tx1, tx2, table);
                TableTool.bugReport.setTx1(tx1);
                TableTool.bugReport.setTx2(tx2);
                log.info("Transaction 1:\n{}", tx1);
                log.info("Transaction 2:\n{}", tx2);
                TrocChecker checker = new TrocChecker(tx1, tx2);
                // 随机生成提交顺序
                checker.checkRandom();
                if (TableTool.txPairHasConflict) {
                    TableTool.conflictTxPair++;
                }
                try {
                    tx1.conn.close();
                    tx2.conn.close();
                } catch (SQLException e) {
                    log.info("Close connection failed.");
                    e.printStackTrace();
                }
            
        
    }

    private static void verifyOptions(Options options) {
        options.setDBMS(options.getDBMS().toUpperCase());
        if (Arrays.stream(DBMS.values()).map(DBMS::name).noneMatch(options.getDBMS()::equals)) {
            throw new RuntimeException("Unknown DBMS: " + options.getDBMS());
        }
    }
}
