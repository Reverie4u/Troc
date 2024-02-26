package troc;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import com.beust.jcommander.JCommander;

import lombok.extern.slf4j.Slf4j;
import troc.common.Table;

@Slf4j
public class test {
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
        List <String> initialStmts = new ArrayList<>();
        ArrayList<StatementCell> txStmts ;

        Reducer reducer = new Reducer();
        log.info("Create new table.");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        Table table = TableTool.dbms.buildTable(options.getTableName());
        table.initialize();
        
        TableTool.preProcessTable();

        RandomMethod tmp = new RandomMethod();
        initialStmts.add("1");
        initialStmts.add("2");
        initialStmts.add("3");
        int cnt = 0;
        for(int i=0;i<=initialStmts.size()-1;i++){
            if(cnt == 0){
             initialStmts.remove(i);
             cnt++;
            }
            System.out.println(initialStmts.get(i));
        }
        
        System.out.println("---------------------------------------------");
  
/*         
        log.info("Initial table:\n{}", TableTool.tableToView());
        initialStmts = table.getInitializeStatements();
        log.info(table.getCreateTableSql());
        for(String stmt:initialStmts){
            log.info(stmt);
        }
    
        log.info("---------------------------------------");
        table.drop();
        TableTool.executeOnTable(table.getCreateTableSql());

        new_initalStmts = reducer.del_Init(initialStmts,0);
        for(String new_stmt:new_initalStmts){
            log.info(new_stmt);
        }
        reducer.intialize(new_initalStmts);
        TableTool.preProcessTable();
        log.info("Initial table:\n{}", TableTool.tableToView());
*/
        // 这个地方已经创建好表了，并填充数据了
        //  TableTool.preProcessTable();
        //  log.info("Initial table:\n{}", TableTool.tableToView());
        // log.info("Generate new transaction pair.");
        /*
        tx1 = table.genTransaction(1);
        tx2 = table.genTransaction(2);
        for(StatementCell txStmt: tx1.statements){
            log.info(txStmt.toString()+"   "+txStmt.statement);
        }
        for(StatementCell txStmt: tx2.statements){
            log.info(txStmt.toString()+"   "+txStmt.statement);
        }   
        log.info("---------------------------------------");   

        tx2 = reducer.delTransaction(tx2, 2);  
        tx1 = reducer.delTransaction(tx1, 1);   
        for(StatementCell txStmt: tx1.statements){
            log.info(txStmt.toString()+"   "+txStmt.statement);
        }        

        for(StatementCell txStmt: tx2.statements){
            log.info(txStmt.toString()+"   "+txStmt.statement);
        }     
        TrocChecker checker = new TrocChecker(tx1, tx2);
        checker.checkRandom(1);
         */
    }

    private static void verifyOptions(Options options) {
        options.setDBMS(options.getDBMS().toUpperCase());
        if (Arrays.stream(DBMS.values()).map(DBMS::name).noneMatch(options.getDBMS()::equals)) {
            throw new RuntimeException("Unknown DBMS: " + options.getDBMS());
        }
    }
}
