package troc;

import java.beans.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import com.mysql.cj.conf.ConnectionUrlParser.Pair;

import troc.common.Table;

public class Reducer {
    private static final StatementType INSERT_IGNORE = null;
    protected String originalCreateStmt; // 简化前的建表语句

    protected List<String> originalInitStmt; // 简化前的建表初始化语句
    protected Transaction originalTx1; // 简化前的事务1
    protected Transaction originalTx2; // 简化前的事务2
    protected ArrayList<StatementCell> originalSchedule; // 简化前的提交顺序

    protected String simplifiedCreateStmt; //简化后的建表语句

    protected List<String> simplifiedInitStmt; // 简化后的建表初始化语句
    protected Transaction simplifiedTx1; // 简化后的事务1
    protected Transaction simplifiedTx2; // 简化后的事务2
    protected ArrayList<StatementCell> simplifiedSchedule; // 简化后的提交顺序

    protected HashMap<String,Double> tableForTx = new HashMap<String, Double>();
    protected HashMap<String,Double> tableForInit = new HashMap<String, Double>();
    // Reducer的带参构造 由于未进行任何操作 将初始化和简化后的参数都进行统一设置
    public Reducer(){}
    public Reducer(String createSql, List<String> InitStmt, Transaction tx1, Transaction tx2, ArrayList<StatementCell> Schedule){
        // 将原始的直接指向原数据地址
        this.originalCreateStmt = createSql;
        this.originalInitStmt = InitStmt;
        this.originalTx1 = tx1;
        this.originalTx2 = tx2;
        this.originalSchedule = Schedule;

        // 将简化的默认值修改重新开辟内存空间拷贝复制
        this.simplifiedCreateStmt = new String();
        this.simplifiedCreateStmt = createSql;
        
        this.simplifiedInitStmt = new ArrayList<String>();
        this.simplifiedInitStmt.addAll(InitStmt);

        this.simplifiedSchedule = new ArrayList<StatementCell>();
        this.simplifiedSchedule.addAll(Schedule);

        this.simplifiedTx1 = this.originalTx1.copyForStmt();
        this.simplifiedTx2 = this.originalTx2.copyForStmt();
        
    }
    public void initTableFroInit(){
        /*
         * 语句类型有：
         *   INSERT INTO
         *   INSERT IGNORE INTO
         *   CREATE INDEX
         *   CREATE UNIQUE INDEX 
         */
        Double probability = (double) (1/4);
        tableForInit.put("INSERT INTO", probability);
        tableForInit.put("INSERT IGNORE INTO", probability);
        tableForInit.put("CREATE INDEX", probability);
        tableForInit.put("CREATE UNIQUE INDEX", probability);
    }
    public void initTableForTx(){
        /*
         * 语句类型有：
         *    UNKNOWN,
         *   SELECT, SELECT_SHARE, SELECT_UPDATE,
         *   UPDATE, DELETE, INSERT, SET,
         *   BEGIN, COMMIT, ROLLBACK,
         */
        Double probability = (double) (1/6);
        tableForTx.put("SELECT", probability);
        tableForTx.put("SELECT_SHARE", probability);
        tableForTx.put("SELECT_UPDATE", probability);
        tableForTx.put("UPDATE", probability);
        tableForTx.put("INSERT", probability);
        tableForTx.put("DELETE", probability);
    }
    // 删除初始化语句InitStmts中第idx个语句 idx范围从0~initStmts.size()-1
    public void delInit(List <String> initStmts, int idx){
        initStmts.remove(idx);
    }
    public void delFlagInit(List<Pair<String,String>> initFlagStmts, int idx){
        initFlagStmts.remove(idx);
    }
    public void delFlagTxStmt(List<Pair<String,StatementCell>> txFlagStmts, int idx){
        txFlagStmts.remove(idx);
    }
    // 对Troc表进行初始化语句的执行
    public void intialize(List<String> initStmts){
        for(String initStmt: initStmts){
            TableTool.executeOnTable(initStmt);
        }
    }

    // 删除事务tx中的第idx个语句 idx范围0~tx.statements.size()-1 
    public void delTransaction(Transaction tx, int idx){
        StatementCell stmt;
        for(int i=idx+1; i<=tx.statements.size()-1;i++){
            stmt = tx.statements.get(i);
            stmt.statementId--;    
        }
        tx.statements.remove(idx);
    }
    // 在事务tx中的第idx位置加入之前删除的语句stmt 并恢复stmtId 
    public void recoverTransaction(Transaction tx, int idx, StatementCell stmt){
        tx.statements.add(idx, stmt);
        StatementCell tmpStmt;
        for(int i = idx+1; i<=tx.statements.size()-1;i++){
            tmpStmt = tx.statements.get(i);
            tmpStmt.statementId++;
        }
    }
    // 在整体提交顺序schedule中的第idx个位置添加事务txId的第stmtId个语句 
    public void recoverSchedule(ArrayList<StatementCell> schedule, int ith, StatementCell stmt){
        schedule.add(ith,stmt);
        for(int i = ith+1; i<=schedule.size()-1;i++){
            StatementCell schStmt = schedule.get(i);
            if(schStmt.tx.txId == stmt.tx.txId && schStmt.statementId >= stmt.statementId){
                schStmt.statementId++;
            }
        }
    }
    // 在整体提交顺序schedule中删除txId事务的第stmtId个语句
    public int delSchedule(ArrayList<StatementCell> schedule, int txId, int stmtId){
        int removeIdx = -1;
        System.out.println(schedule);
        for(int i = 0 ;i <= schedule.size()-1; i++){
            StatementCell stmt = schedule.get(i);
            if(stmt.tx.txId == txId && stmt.statementId == stmtId){     
                removeIdx = i;
                break;
            }
        }
        
        for(int i = 0 ;i<= schedule.size()-1; i++){
            StatementCell stmt = schedule.get(i);
            if(stmt.tx.txId == txId && stmt.statementId > stmtId){     
                stmt.statementId--;
            }
         //   System.out.println(schedule);
        }
        return removeIdx;
    }
    // 将事务tx中第idx个语句从整体提交顺序schedule中删除
    public int reduceTransactionSchedule(ArrayList<StatementCell> schedule, Transaction tx, int idx){
        delTransaction(tx, idx);
        return delSchedule(schedule, tx.txId, idx);
    }

    public void recoverTransactionSchedule(ArrayList<StatementCell> schedule, Transaction tx, int idx, int ithSchedule, StatementCell stmt){
        recoverTransaction(tx,idx,stmt);
        recoverSchedule(schedule,ithSchedule,stmt.copy());
    }    
    // 去除语句规则简化
    public void reduceRowRule(){
        System.out.println("ReduceRowRule-----------------------------------------------------");
        // 建立临时变量 左边标记是否选择 右边标记句子
        List<Pair<String,String>> tmpInitStmt = new ArrayList<>();
        List<Pair<String,StatementCell>> tmpTx1Stmt = new ArrayList<>();
        List<Pair<String,StatementCell>> tmpTx2Stmt = new ArrayList<>();

        Transaction tmpTx1 = this.simplifiedTx1.copyForStmt();
        Transaction tmpTx2 = this.simplifiedTx2.copyForStmt(); 

        for(String stmt:this.simplifiedInitStmt){
            Pair<String,String> flagStmt = new Pair<String,String>("Unselected", stmt);
            tmpInitStmt.add(flagStmt);
        }
      
        for(StatementCell txStmt: tmpTx1.statements){
            // 看看是不是 BEGIN ROLLBACK COMMIT 如果是则直接进行标记 因为不会将他们进行简化
            if(txStmt.type.toString().equals("COMMIT") || txStmt.type.toString().equals("ROLLBACK") || txStmt.type.toString().equals("BEGIN")){
                Pair<String,StatementCell> flagTx1Stmt = new Pair<String,StatementCell>("Selected", txStmt);
                tmpTx1Stmt.add(flagTx1Stmt);
            }
            else{
                Pair<String,StatementCell> flagTx1Stmt = new Pair<String,StatementCell>("Unselected", txStmt);
                tmpTx1Stmt.add(flagTx1Stmt);
             }        
        }  

        for(StatementCell txStmt: tmpTx2.statements){
            if(txStmt.type.toString().equals("COMMIT") || txStmt.type.toString().equals("ROLLBACK") || txStmt.type.toString().equals("BEGIN")){
                Pair<String,StatementCell> flagTx2Stmt = new Pair<String,StatementCell>("Selected", txStmt);
                tmpTx2Stmt.add(flagTx2Stmt);
            }     
            else{
                Pair<String,StatementCell> flagTx2Stmt = new Pair<String,StatementCell>("Unselected", txStmt);
                tmpTx2Stmt.add(flagTx2Stmt);
             }
             
        }

        ArrayList<StatementCell> tmpSchedule = new ArrayList<StatementCell>();
        tmpSchedule.addAll(this.simplifiedSchedule);

        // 打印提交顺序
        System.out.println(tmpSchedule);
        System.out.println("-------------------------------------------------------");
       
        // 打印初始化语句
        Integer cnt = -1;
        for(Pair<String,String> tmp : tmpInitStmt){
            ++cnt ;
            System.out.println(cnt.toString()+": "+tmp.right+" "+tmp.left);
        }


        System.out.println("Init Stmt-------------------------------------------------------");
        
        RandomMethod method = new RandomMethod();
        //  获取方法
        boolean initFlag = true;
        
        // 简化初始化语句
        while (initFlag) {
          if(tmpInitStmt.size() == 1) break;
          // 获取方法下得到的语句类型
          String type = method.selectMethodForInit();
          System.out.println(type);
          String[] typeWords = type.split(" ");            
          for(int i = 0; i <= tmpInitStmt.size()-1; i++){
            // 获得带有标记的初始化语句 左边是标记 右边是句子
            Pair<String,String> initStmt = tmpInitStmt.get(i);
            if(!hasUnselectedInitStmt(tmpInitStmt)){
              initFlag = false;
              break;
            }
            if(initStmt.left.equals("Selected")) 
              continue;

            String[] initWords = initStmt.right.split(" ");
            // 判断该语句是否符合要求的语句类型
            if(initWords[0].equals(typeWords[0]) && initWords[1].equals(typeWords[1])){
                // 符合 则将语句删除简化
                delFlagInit(tmpInitStmt, i);
                // 判断是否简化后产生BUG 如果产生 更新简化后的语句并Break
                if(!oralceCompare(simplifiedCreateStmt,tmpInitStmt,tmpTx1,tmpTx2,tmpSchedule)){
                   // updateSimplifiedInitStmt(tmpInitStmt);
                    break;
                }
                // 如果没产生BUG 则把删除的语句加载到原来的位置
                else{
                    Pair<String,String> selectedInitStmt = new Pair<String,String>("Selected", initStmt.right);
                    if(hasUnselectedInitStmt(tmpInitStmt))
                      tmpInitStmt.add(i, selectedInitStmt);
                    else{
                        tmpInitStmt.add(i, selectedInitStmt);
                        initFlag = false;
                        break;
                    }            
                }
            }
          }
            cnt = -1;
            for(Pair<String,String> tmp : tmpInitStmt){
                ++cnt ;
                System.out.println(cnt.toString()+": "+tmp.right+" "+tmp.left);
                
            }
            System.out.println("-------------------------------------------------------");
        }    
        // 更新最简的初始化语句
        updateSimplifiedInitStmt(tmpInitStmt);

          for(Pair<String,StatementCell> ptStmt:tmpTx1Stmt){
            System.out.println(ptStmt.right.statementId+": "+ptStmt.right.stmtToString()+" "+ptStmt.left);
          }
          System.out.println("Tx1 -------------------------------------------------------");
        // 简化事务1语句
        boolean txFlag = true;
        while (txFlag) {
            // 获得带有标记的事务语句
            String typeTx = method.selectMethodForTx();

            System.out.println(typeTx);

            for(int i = 0; i<= tmpTx1Stmt.size()-1; i++){
                Pair<String, StatementCell> tx1Stmt = tmpTx1Stmt.get(i);
                String tx1StmtType = tx1Stmt.right.type.toString();
                // 如果都被选择了就结束了
                if(!hasUnselectedTxStmt(tmpTx1Stmt)){
                    txFlag = false;
                    break;
                }
                // 如果该语句被选择了就跳过
                if(tx1Stmt.left.equals("Selected"))
                  continue;
                // 判断是否是符合要求类型的没选择的语句
                if(typeTx.equals(tx1StmtType)){
                    // 符合 则将语句删除进行简化 只对事务Tx和提交顺序进行操作 不修改带标记的Tx1Stmt 
                    
                    int delIth =  reduceTransactionSchedule(tmpSchedule, tmpTx1, i);
                    StatementCell removeStmt = tmpSchedule.remove(delIth).copy();
                    // 判断是否简化后产生Bug 如果产生 可选择更新简化后的事务语句并Break
                    if(!oralceCompare(simplifiedCreateStmt,tmpInitStmt, tmpTx1, tmpTx2, tmpSchedule)){
                        // 有BUG 则将该句从带标记Tx1Stmt中删除
                        delFlagTxStmt(tmpTx1Stmt, i);
                        break;
                    }
                    // 如果没产生BUG 就把删除的语句恢复到原来位置 并且临时事务tx中的序号也得变回去
                    else{
                        // 先恢复事务Tx和提交顺序至删除前的状态 
                        recoverTransactionSchedule(tmpSchedule, tmpTx1, i, delIth,removeStmt);
                        Pair<String,StatementCell> selectedTxStmt = new Pair<String,StatementCell>("Selected", removeStmt);
                        if(hasUnselectedTxStmt(tmpTx1Stmt)){
                            tmpTx1Stmt.remove(i); // 把原本的Unselected变更为Selected 
                            tmpTx1Stmt.add(i,selectedTxStmt);
                        }
                        else{
                            txFlag = false;
                            break;
                        }
                    }
                  }
                }

                for(Pair<String,StatementCell> ptStmt:tmpTx1Stmt)
                  System.out.println(ptStmt.right.statementId+": "+ptStmt.right.stmtToString()+" "+ptStmt.left);
                
                System.out.println(tmpSchedule);
                System.out.println("-----------------------------------------------------");
        }
        updateSimplifiedTx1Stmt(tmpTx1);
        updateSimplifiedSchedule(tmpSchedule);
        System.out.println(this.simplifiedTx1);
        System.out.println("Tx2 -----------------------------------------------------");
        cnt = -1;
        // for(StatementCell pStmt:tmpSchedule){
        //   ++cnt ;
        //   System.out.println(cnt.toString()+": "+pStmt.stmtToString()+" "+pStmt.tx.txId+"-"+pStmt.statementId);
        // }
        System.out.println(this.simplifiedSchedule);
        boolean tx2Flag = true;
        while (tx2Flag) {
            // 获得带有标记的事务语句
            String typeTx = method.selectMethodForTx();
            System.out.println(typeTx);
            for(int i = 0; i<= tmpTx2Stmt.size()-1; i++){
                Pair<String, StatementCell> tx2Stmt = tmpTx2Stmt.get(i);
                String tx2StmtType = tx2Stmt.right.type.toString();
                // 如果都被选择了就结束了
                if(!hasUnselectedTxStmt(tmpTx2Stmt)){
                    tx2Flag = false;
                    break;
                }
                // 如果该语句被选择了就跳过
                if(tx2Stmt.left.equals("Selected"))
                  continue;
                // 判断是否是符合要求类型的没选择的语句
                if(typeTx.equals(tx2StmtType)){
                    // 符合 则将语句删除进行简化 只对事务Tx和提交顺序进行操作 不修改带标记的Tx1Stmt 
                    int delIth =  reduceTransactionSchedule(tmpSchedule, tmpTx2, i);
                    StatementCell removeStmt = tmpSchedule.remove(delIth);
                    // 判断是否简化后产生Bug 如果产生 可选择更新简化后的事务语句并Break
                    if(!oralceCompare(simplifiedCreateStmt,tmpInitStmt, tmpTx1, tmpTx2, tmpSchedule)){
                        // 有BUG 则将该句从带标记Tx1Stmt中删除
                        delFlagTxStmt(tmpTx2Stmt, i);
                        break;
                    }
                    // 如果没产生BUG 就把删除的语句恢复到原来位置 并且临时事务tx中的序号也得变回去
                    else{
                        // 先恢复事务Tx和提交顺序至删除前的状态 
                        recoverTransactionSchedule(tmpSchedule, tmpTx2, i, delIth,removeStmt);
                        Pair<String,StatementCell> selectedTx2Stmt = new Pair<String,StatementCell>("Selected", removeStmt);
                        if(hasUnselectedTxStmt(tmpTx2Stmt)){
                            tmpTx2Stmt.remove(i); // 把原本的Unselected变更为Selected 
                            tmpTx2Stmt.add(i,selectedTx2Stmt);
                        }
                        else{
                            tx2Flag = false;
                            break;
                        }
                    }
                  }
                }
              
                for(Pair<String,StatementCell> ptStmt:tmpTx2Stmt)
                  System.out.println(ptStmt.right.statementId+": "+ptStmt.right.stmtToString()+" "+ptStmt.left);
                  System.out.println(tmpSchedule);
                  System.out.println("-----------------------------------------------------");
        }
        updateSimplifiedTx2Stmt(tmpTx2);
        updateSimplifiedSchedule(tmpSchedule);
        System.out.println(tmpSchedule);
        System.out.println("-----------------------------------------------------");
        cnt = -1;
        for(String sipInitStmt:simplifiedInitStmt){
            ++cnt;
            System.out.println(cnt.toString()+":"+sipInitStmt);
        }
        System.out.println(simplifiedTx1);
        System.out.println(simplifiedTx2);
        System.out.println(simplifiedSchedule);
        System.out.println("ReduceRowRule-----------------------------------------------------");
    }
    public void reduceWhereRule(){
        System.out.println("ReduceWhereRule---------------------------------------------------");
        List<Pair<String,StatementCell>> tmpTx1Stmt = new ArrayList<>();
        List<Pair<String,StatementCell>> tmpTx2Stmt = new ArrayList<>();

        Transaction tmpTx1 = this.simplifiedTx1.copyForStmt();
        Transaction tmpTx2 = this.simplifiedTx2.copyForStmt();
        // 因为INSERT BEGIN COMMIT ROLLBACK语句不包含WHERE子句 所以直接进行标记即可
        for(StatementCell stmt:tmpTx1.statements){
            if(stmt.type.toString().equals("INSERT") || 
               stmt.type.toString().equals("BEGIN")  || 
               stmt.type.toString().equals("COMMIT") ||
               stmt.type.toString().equals("ROLLBACK")){
                Pair<String,StatementCell> flagTx1Stmt =  new Pair<String,StatementCell>("Selected", stmt);
                tmpTx1Stmt.add(flagTx1Stmt);
               }
            else{
                Pair<String,StatementCell> flagTx1Stmt =  new Pair<String,StatementCell>("Unselected", stmt);
                tmpTx1Stmt.add(flagTx1Stmt);
            }
        }
        for(StatementCell stmt:tmpTx2.statements){
            if(stmt.type.toString().equals("INSERT") || 
               stmt.type.toString().equals("BEGIN")  || 
               stmt.type.toString().equals("COMMIT") ||
               stmt.type.toString().equals("ROLLBACK")){
                Pair<String,StatementCell> flagTx2Stmt =  new Pair<String,StatementCell>("Selected", stmt);
                tmpTx2Stmt.add(flagTx2Stmt);
               }
            else{
                Pair<String,StatementCell> flagTx2Stmt =  new Pair<String,StatementCell>("Unselected", stmt);
                tmpTx2Stmt.add(flagTx2Stmt);
            }
        }
        for(Pair<String,StatementCell> stmt:tmpTx1Stmt){
            System.out.println(stmt.right.statement+" "+stmt.left);
        }
        System.out.println("---------------------------------------------------------------");
        for(int i = 0; i<=tmpTx1Stmt.size()-1; i++){
            
            Pair<String,StatementCell> tx1Stmt = new Pair<String,StatementCell>(null, null);
            tx1Stmt = tmpTx1Stmt.get(i);
            if(tx1Stmt.left.equals("Selected")) 
             continue;

            StatementCell tmpStmtCellTx1 = tx1Stmt.right.copy();
       //     System.out.println(tx1Stmt.right.type.toString());
            if(tx1Stmt.right.type.toString().equals("SELECT_SHARE")){
                int lockIdx = tx1Stmt.right.statement.indexOf("LOCK");
                int whereIdx = tx1Stmt.right.statement.indexOf("WHERE");
                tx1Stmt.right.statement = tx1Stmt.right.statement.substring(0, whereIdx)+tx1Stmt.right.statement.substring(lockIdx);
                tx1Stmt.right.whereClause = "";
            }
            else if(tx1Stmt.right.type.toString().equals("SELECT_UPDATE")){
                int forIdx = tx1Stmt.right.statement.indexOf("FOR");
                int whereIdx = tx1Stmt.right.statement.indexOf("WHERE");
                tx1Stmt.right.statement = tx1Stmt.right.statement.substring(0, whereIdx)+tx1Stmt.right.statement.substring(forIdx);
                tx1Stmt.right.whereClause = "";
            }
            else{
              String tx1DeleteWhere = tx1Stmt.right.wherePrefix;
              tx1Stmt.right.statement = tx1DeleteWhere;
              tx1Stmt.right.whereClause = "";
            }
            tmpTx1.statements.remove(i);
            tmpTx1.statements.add(i,tx1Stmt.right);

            if(!hasBugForWhereRule(simplifiedCreateStmt, simplifiedInitStmt, tmpTx1, tmpTx2, simplifiedSchedule)){
                tmpTx1.statements.remove(i);
                tmpTx1.statements.add(i,tmpStmtCellTx1);
                Pair<String,StatementCell> selectedTx1Stmt = new Pair<String,StatementCell>("Selected", tmpStmtCellTx1);
                tmpTx1Stmt.remove(i);
                tmpTx1Stmt.add(i,selectedTx1Stmt);
            }

          //  System.out.println(tx1Stmt.right.whereClause +" //  "+tx1Stmt.right.statement+" "+tx1Stmt.left);
        }
        System.out.println(tmpTx1);
        this.simplifiedTx1 = tmpTx1.copyForStmt();
        System.out.println("Tx2 origin ---------------------------------------------------------------");   
    for(Pair<String,StatementCell> stmt:tmpTx2Stmt){
        System.out.println(stmt.right.statement+" "+stmt.left);
    }
    System.out.println("Tx2---------------------------------------------------------------");
    for(int i = 0; i<=tmpTx2Stmt.size()-1; i++){
        
        Pair<String,StatementCell> tx2Stmt = tmpTx2Stmt.get(i);
        if(tx2Stmt.left.equals("Selected")) 
         continue;

        StatementCell tmpStmtCellTx2 = tx2Stmt.right.copy();
   //     System.out.println(tx1Stmt.right.type.toString());
        if(tx2Stmt.right.type.toString().equals("SELECT_SHARE")){
            int lockIdx = tx2Stmt.right.statement.indexOf("LOCK");
            int whereIdx = tx2Stmt.right.statement.indexOf("WHERE");
            tx2Stmt.right.statement = tx2Stmt.right.statement.substring(0, whereIdx)+tx2Stmt.right.statement.substring(lockIdx);
            tx2Stmt.right.whereClause = "";
        }
        else if(tx2Stmt.right.type.toString().equals("SELECT_UPDATE")){
            int forIdx = tx2Stmt.right.statement.indexOf("FOR");
            int whereIdx = tx2Stmt.right.statement.indexOf("WHERE");
            tx2Stmt.right.statement = tx2Stmt.right.statement.substring(0, whereIdx)+tx2Stmt.right.statement.substring(forIdx);
            tx2Stmt.right.whereClause = "";
        }
        else{
          String tx2DeleteWhere = tx2Stmt.right.wherePrefix;
          tx2Stmt.right.statement = tx2DeleteWhere;
          tx2Stmt.right.whereClause = "";
        }
        tmpTx2.statements.remove(i);
        tmpTx2.statements.add(i,tx2Stmt.right);

        if(!hasBugForWhereRule(simplifiedCreateStmt, simplifiedInitStmt, tmpTx1, tmpTx2, simplifiedSchedule)){
            tmpTx2.statements.remove(i);
            tmpTx2.statements.add(i,tmpStmtCellTx2);
            Pair<String,StatementCell> selectedTx2Stmt = new Pair<String,StatementCell>("Selected", tmpStmtCellTx2);
            tmpTx2Stmt.remove(i);
            tmpTx2Stmt.add(i,selectedTx2Stmt);
         }
       }
      
       System.out.println(tmpTx2);
       
       this.simplifiedTx2 = tmpTx2.copyForStmt();
       System.out.println(this.originalTx1);
       System.out.println(this.simplifiedTx1);
       System.out.println(this.originalTx2);
       System.out.println(this.simplifiedTx2);
        System.out.println("ReduceWhereRule---------------------------------------------------");
        
    } 
    public boolean hasUnselectedInitStmt(List<Pair<String,String>> tmpInitStmt){
        boolean flag = false;
        for(Pair<String,String> flagInitStmt:tmpInitStmt){
            if(flagInitStmt.left.equals("Unselected")){
                flag = true;
                break;
            }
        }
        return flag;
    }
    public boolean hasUnselectedTxStmt(List<Pair<String,StatementCell>> tmpTxStmt){
        boolean flag = false;
        for(Pair<String,StatementCell> flagTxStmt:tmpTxStmt){
            if(flagTxStmt.left.equals("Unselected")){
                flag = true;
                break;
            }
        }
        return flag;
    }
    public void updateSimplifiedInitStmt(List<Pair<String,String>> newInitStmt){
        this.simplifiedInitStmt.clear();
        for(Pair<String,String> flagStmt: newInitStmt){
            this.simplifiedInitStmt.add(flagStmt.right);
        }
    }    
    public void updateSimplifiedTx1Stmt(Transaction tmpTx){
        this.simplifiedTx1 = tmpTx.copyForStmt();
    }   
    public void updateSimplifiedTx2Stmt(Transaction tmpTx){
        this.simplifiedTx2 = tmpTx.copyForStmt();
    }  
    public void updateSimplifiedSchedule(ArrayList<StatementCell> tmpSchedule){
        this.simplifiedSchedule.clear();
        this.simplifiedSchedule.addAll(tmpSchedule);
    }
    public boolean oralceCompare(String createSql,List<Pair<String,String>> flagInitStmt, Transaction tx1, Transaction tx2, ArrayList<StatementCell> schedule){
       // System.out.println("HasBug----------------------------------------");
    //   System.out.println(TableTool.tableToView().toString());
        System.out.println(createSql);
        String newCreateSql = createSql.replaceFirst("t", "t1");
        System.out.println(newCreateSql);
        System.out.println("--------------------------------------------------");
   //     Table table = TableTool.dbms.buildTable("t1");
        TableTool.TableName = "t1";

        TableTool.executeOnTable(newCreateSql);
        int cnt = -1;
      //  System.out.println(TableTool);
        for(Pair<String,String> initStmt : flagInitStmt){
            String newInitSQL = initStmt.right.replaceFirst("t", "t1");
            ++cnt;
            System.out.println(cnt+":"+newInitSQL);
            TableTool.executeOnTable(newInitSQL);
        }
        TableTool.addRowIdColumnAndFill(); 
        TableTool.fillTableMetaData();

        System.out.println(TableTool.tableToView().toString());
   //     System.out.println(table.toString());
        TrocChecker checker = new TrocChecker(tx1, tx2);
        TxnPairExecutor newExecutor = new TxnPairExecutor(scheduleClone(schedule), tx1, tx2);
        TxnPairResult execResult = newExecutor.getResult();
        TxnPairResult mvccResult = checker.inferOracleMVCCForReduce(scheduleClone(schedule));
        // Random rand = new Random();
        // Integer idx = rand.nextInt(10)+1;  
        // System.out.println("hasBug idx:"+idx.toString());
        // System.out.println("------------------------------------------");
        // if(idx<=3) return true;
        // else       return false; 
        boolean res = checker.compareOracles(execResult, mvccResult);
        TableTool.clearTable("t1");
        TableTool.TableName = "t";
        TableTool.recoverOriginalTable();
        System.out.println(TableTool.tableToView().toString());
        return res;
    }
    public boolean hasBugForWhereRule(String createSql,List<String> flagInitStmt, Transaction tx1, Transaction tx2, ArrayList<StatementCell> schedule){
       System.out.println("HasBug----------------------------------------");
        // System.out.println(createSql);
        // String newCreateSql = createSql.replace("t", "t1");
        // System.out.println(newCreateSql);
        // Table table = TableTool.dbms.buildTable("t1");
        // TableTool.executeOnTable(newCreateSql);
        
        // for(String initStmt : flagInitStmt){
        //     System.out.println(initStmt);
        //     TableTool.executeOnTable(initStmt);
        // }
        // TableTool.preProcessTable();
        // System.out.println(TableTool.tableToView().toString());

        // TxnPairExecutor newExecutor = new TxnPairExecutor(scheduleClone(schedule), tx1, tx2);
        // TxnPairResult execResult = newExecutor.getResult();
        // TxnPairResult mvccResult = inferOracleMVCC(scheduleClone(schedule));
        
        Random rand = new Random();
        Integer idx = rand.nextInt(10)+1;  
        System.out.println("hasBug idx:"+idx.toString());
       // System.out.println("------------------------------------------");
        if(idx<=3) return true;
        else       return false; 
        
    }

    private ArrayList<StatementCell> scheduleClone(ArrayList<StatementCell> schedule) {
        ArrayList<StatementCell> copied = new ArrayList<>();
        for (StatementCell stmt : schedule) {
            copied.add(stmt.copy());
        }
        return copied;
    }
    public List<String> getOriginalInitStmt(){
        return originalInitStmt;
    }

    public Transaction getOriginalTx1(){
        return originalTx1;
    }

    public Transaction getOriginalTx2(){
        return originalTx2;
    }

    public ArrayList<StatementCell> getOriginalSchedule(){
        return originalSchedule;
    }

    public List<String> getSimplifiedInitStmt(){
        return simplifiedInitStmt;
    }

    public Transaction getSimplifiedTx1(){
        return simplifiedTx1;
    }

    public Transaction getSimplifiedTx2(){
        return simplifiedTx2;
    }

    public ArrayList<StatementCell> getSimplifiedSchedule(){
        return simplifiedSchedule;
    }
}
