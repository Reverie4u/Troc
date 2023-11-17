package troc;

import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

@Slf4j
public class TxnPairExecutor {

    private final ArrayList<StatementCell> submittedOrder;
    private final Transaction tx1;
    private final Transaction tx2;

    private TxnPairResult result;
    private ArrayList<StatementCell> actualSchedule;
    private ArrayList<Object> finalState;

    private boolean isDeadLock = false;
    private boolean timeout = false;
    private String exceptionMessage = "";
    private final Map<Integer, Boolean> txnAbort = new HashMap<>();

    public TxnPairExecutor(ArrayList<StatementCell> schedule, Transaction tx1, Transaction tx2) {
        this.submittedOrder = schedule;
        this.tx1 = tx1;
        this.tx2 = tx2;
    }

    TxnPairResult getResult() {
        if (result == null) {
            execute();
            result = new TxnPairResult(actualSchedule, finalState, isDeadLock);
        }
        return result;
    }

    private void execute() {
        TableTool.setIsolationLevel(tx1);
        TableTool.setIsolationLevel(tx2);
        actualSchedule = new ArrayList<>();
        txnAbort.put(1, false);
        txnAbort.put(2, false);
        BlockingQueue<StatementCell> queue1 = new SynchronousQueue<>();
        BlockingQueue<StatementCell> queue2 = new SynchronousQueue<>();
        BlockingQueue<StatementCell> communicationID = new SynchronousQueue<>();
        Thread producer = new Thread(new Producer(queue1, queue2, submittedOrder, communicationID));
        Thread consumer1 = new Thread(new Consumer(1, queue1, communicationID, submittedOrder.size()));
        Thread consumer2 = new Thread(new Consumer(2, queue2, communicationID, submittedOrder.size()));
        producer.start();
        consumer1.start();
        consumer2.start();
        try {
            // 等待producer子线程返回
            producer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finalState = TableTool.getQueryResultAsList("SELECT * FROM " + TableTool.TableName);
    }
    /**
     * Producer按照schedule中的顺序给两个队列分发任务，如果队列阻塞则将任务放入一个阻塞队列中
     * 1.对于 schedule 中的每个任务，首先检查对应的线程是否被阻塞。如果被阻塞，那么将任务添加到阻塞任务列表中，然后继续处理下一个任务。
       2.如果线程没有被阻塞，那么将任务添加到对应的队列中，然后等待线程的反馈。
       3.如果线程在一定时间内没有反馈，那么认为线程被阻塞，将任务添加到阻塞任务列表中。
       4.如果线程有反馈，那么根据反馈的内容处理任务。如果任务是提交或回滚操作，那么还需要处理另一个线程的阻塞情况。
       5.如果出现死锁，那么停止所有的事务，并结束线程的运行。
       6.在所有的任务都处理完之后，发送停止信号给所有的线程。
     */
    class Producer implements Runnable {
        private final BlockingQueue<StatementCell> queue1;
        private final BlockingQueue<StatementCell> queue2;
        private final ArrayList<StatementCell> schedule;
        private final BlockingQueue<StatementCell> communicationID; // represent the execution feedback

        public Producer(BlockingQueue<StatementCell> queue1, BlockingQueue<StatementCell> queue2,
                ArrayList<StatementCell> schedule, BlockingQueue<StatementCell> communicationID) {
            this.queue1 = queue1;
            this.queue2 = queue2;
            this.schedule = schedule;
            this.communicationID = communicationID;
        }

        public void run() {
            Map<Integer, Boolean> txnBlock = new HashMap<>(); // whether a child thread is blocked
            txnBlock.put(1, false); // txn 1 is false
            txnBlock.put(2, false);
            Map<Integer, ArrayList<StatementCell>> blockedStmts = new HashMap<>(); // record blocked statements
            blockedStmts.put(1, null);
            blockedStmts.put(2, null);
            Map<Integer, BlockingQueue<StatementCell>> queues = new HashMap<>(); // record queues
            queues.put(1, queue1);
            queues.put(2, queue2);
            int queryID; // statement ID
            long startTs; // record time threshold
            timeout = false;
            isDeadLock = false;
            out:
            for (queryID = 0; queryID < schedule.size(); queryID++) {
                int txn = schedule.get(queryID).tx.txId;
                StatementCell statementCell = schedule.get(queryID).copy();
                int otherTxn = (txn == 1) ? 2 : 1;
                // 如果当前事务被阻塞，则将当前语句放入阻塞队列中
                if (txnBlock.get(txn)) { // if a child thread is blocked
                    ArrayList<StatementCell> stmts = blockedStmts.get(txn);
                    stmts.add(statementCell);
                    blockedStmts.put(txn, stmts); // save its statements 这里似乎不太必要   
                    continue;
                }
                try {
                    // 当前事务未被阻塞，将当前语句放入队列中
                    queues.get(txn).put(statementCell);
                } catch (InterruptedException e) {
                    log.info(" -- MainThread run exception");
                    log.info("Query: " + statementCell.statement);
                    log.info("Interrupted Exception: " + e.getMessage());
                }
                // 等待语句执行结果
                StatementCell queryReturn = communicationID.poll(); // communicate with a child thread
                startTs = System.currentTimeMillis();
                while (queryReturn == null) { // wait for 2s
                    if (System.currentTimeMillis() - startTs > 2000) { // child thread is blocked
                        log.info(txn + "-" + statementCell.statementId + ": time out");
                        txnBlock.put(txn, true); // record blocked transaction
                        StatementCell blockPoint = statementCell.copy();
                        blockPoint.blocked = true;
                        actualSchedule.add(blockPoint);
                        ArrayList<StatementCell> stmts = new ArrayList<>();
                        stmts.add(statementCell);
                        blockedStmts.put(txn, stmts);
                        break;
                    }
                    queryReturn = communicationID.poll();
                }
                // 成功收到了feedback, 将语句结果放入actualSchedule中
                if (queryReturn != null) { // success to receive feedback
                    if ((statementCell.type == StatementType.COMMIT || statementCell.type == StatementType.ROLLBACK)
                            && txnBlock.get(otherTxn)) {
                        StatementCell nextReturn = communicationID.poll();
                        while (nextReturn == null) {
                            if (System.currentTimeMillis() - startTs > 15000) { // child thread is blocked
                                log.info(" -- " + txn + "." + statementCell.statementId + ": time out");
                                timeout = true;
                                break;
                            }
                            nextReturn = communicationID.poll();
                        }
                        if (nextReturn != null) {
                            if (queryReturn.statement.equals(statementCell.statement)) {
                                statementCell.result = queryReturn.result;
                                blockedStmts.get(otherTxn).get(0).result = nextReturn.result;
                            } else {
                                statementCell.result = nextReturn.result;
                                blockedStmts.get(otherTxn).get(0).result = queryReturn.result;
                            }
                            actualSchedule.add(statementCell);
                            actualSchedule.add(blockedStmts.get(otherTxn).get(0));
                        } else {
                            log.info(" -- next return failed: " + statementCell.statement);
                            break;
                        }
                    } else if (queryReturn.statement.equals(statementCell.statement)) {
                        statementCell.result = queryReturn.result;
                        actualSchedule.add(statementCell);
                    } else {
                        isDeadLock = true;
                        log.info(" -- DeadLock happened(1)");
                        statementCell.blocked = true;
                        actualSchedule.add(statementCell);
                        break out;
                    }

                }
                if ((statementCell.type == StatementType.COMMIT
                        || statementCell.type == StatementType.ROLLBACK) && !exceptionMessage.contains("Deadlock") && !exceptionMessage.contains("lock=true")
                        && !(txnBlock.get(1) && txnBlock.get(2))) {
                    txnBlock.put(otherTxn, false);
                    if (blockedStmts.get(otherTxn) != null) {
                        for (int j = 1; j < blockedStmts.get(otherTxn).size(); j++) {
                            StatementCell blockedStmtCell = blockedStmts.get(otherTxn).get(j);
                            try {
                                queues.get(otherTxn).put(blockedStmtCell);
                            } catch (InterruptedException e) {
                                log.info(" -- MainThread blocked exception");
                                log.info("Query: " + statementCell.statement);
                                log.info("Interrupted Exception: " + e.getMessage());
                            }
                            StatementCell blockedReturn = communicationID.poll();
                            startTs = System.currentTimeMillis();
                            while (blockedReturn == null) {
                                if (System.currentTimeMillis() - startTs > 10000) {
                                    log.info(" -- " + txn + "." + statementCell.statementId + ": still time out");
                                    timeout = true;
                                    break;
                                }
                                blockedReturn = communicationID.poll();
                            }
                            if (blockedReturn != null) {
                                blockedStmtCell.result = blockedReturn.result;
                                actualSchedule.add(blockedStmtCell);

                            }
                        }
                    }
                }
                if (exceptionMessage.length() > 0 || (txnBlock.get(1) && txnBlock.get(2)) || timeout) {
                    if (exceptionMessage.contains("Deadlock") || exceptionMessage.contains("lock=true")
                            || (txnBlock.get(1) && txnBlock.get(2))) { // deadlock
                        log.info(" -- DeadLock happened(2)");
                        isDeadLock = true;
                        break;
                    }
                    if (exceptionMessage.contains("restart") || exceptionMessage.contains("aborted")
                            || exceptionMessage.contains("TransactionRetry")) {
                        txnAbort.put(txn, true);
                        statementCell.aborted = true;
                    }
                }
            }
            if (isDeadLock) {
                try {
                    tx1.conn.createStatement().executeUpdate("ROLLBACK"); // stop transaction
                    tx2.conn.createStatement().executeUpdate("ROLLBACK");
                } catch (SQLException e) {
                    log.info(" -- Deadlock Commit Failed");
                }
                log.info(" -- schedule execute failed");
            }
            StatementCell stopThread1 = new StatementCell(tx1, schedule.size());
            StatementCell stopThread2 = new StatementCell(tx2, schedule.size());
            try {
                while (communicationID.poll() != null);
            } catch (Exception ignored) {}
            try {
                queue1.put(stopThread1);
                queue2.put(stopThread2);
            } catch (InterruptedException e) {
                log.info(" -- MainThread stop child thread Interrupted exception: " + e.getMessage());
            }
        }
    }

    /**
     * Consumer 类的 run 方法是线程的主体函数，它从队列中获取任务并执行，然后将执行结果反馈给主线程。
     * 1.在一个无限循环中，首先从队列中获取一个任务。
     * 2.如果任务的ID大于或等于 scheduleCount，那么跳出循环，结束线程的运行。
     * 3.执行任务，如果任务是查询操作，那么获取查询结果；如果任务是更新操作，那么执行更新。
     * 4.如果执行过程中出现异常，那么记录异常信息。
     * 5.将执行结果放入 communicationID 队列中，通知主线程。
     */
    class Consumer implements Runnable {
        private final BlockingQueue<StatementCell> queue;
        private final BlockingQueue<StatementCell> communicationID; // represent the execution feedback
        private final int scheduleCount;
        private final int consumerId;

        public Consumer(int consumerId, BlockingQueue<StatementCell> queue,
                        BlockingQueue<StatementCell> communicationID, int scheduleCount) {
            this.consumerId = consumerId;
            this.queue = queue;
            this.communicationID = communicationID;
            this.scheduleCount = scheduleCount;
        }

        public void run() {
            try {
                while (true) {
                    StatementCell stmt = queue.take(); // communicate with main thread
                    if (stmt.statementId >= scheduleCount) break; // stop condition: schedule.size()
                    // execute a query
                    String query = stmt.statement;
                    try {
                        if (stmt.type == StatementType.SELECT || stmt.type == StatementType.SELECT_SHARE
                                || stmt.type == StatementType.SELECT_UPDATE) {
                            stmt.result = TableTool.getQueryResultAsListWithException(stmt.tx.conn, query);
                        } else {
                            stmt.tx.conn.createStatement().executeUpdate(query);
                        }
                        exceptionMessage = "";
                    } catch (SQLException e) {
                        log.info(" -- TXNThread threadExec exception");
                        log.info("Query {}: {}", stmt, query);
                        exceptionMessage = e.getMessage();
                        log.info("SQL Exception: " + exceptionMessage);
                        exceptionMessage = exceptionMessage + "; [Query] " + query;
                    } finally {
                        try {
                            // communicationID是一个阻塞队列
                            communicationID.put(stmt); // communicate to main thread
                        } catch (InterruptedException e) { // communicationID.put()
                            log.info(" -- TXNThread threadExec exception");
                            log.info("Query {}: {}", stmt, query);
                            log.info("Interrupted Exception: " + e.getMessage());
                        }
                    }
                }
            } catch (InterruptedException e) {
                // thread stop
                log.info(" -- TXNThread run Interrupted exception: " + e.getMessage());
            }
        }
    }
}
