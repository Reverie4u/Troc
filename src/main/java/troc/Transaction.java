package troc;

import java.util.ArrayList;

import lombok.Data;

@Data
public class Transaction {
    int txId;
    SQLConnection conn;
    IsolationLevel isolationlevel;
    ArrayList<StatementCell> statements;

    // states for analysis
    ArrayList<Transaction> snapTxs;
    View snapView;
    boolean blocked;
    boolean committed;
    boolean finished;
    boolean aborted;
    ArrayList<Lock> locks;
    ArrayList<StatementCell> blockedStatements;

    public Transaction(int txId) {
        this.txId = txId;
        statements = new ArrayList<>();
    }

    public Transaction(int txId, IsolationLevel isolationlevel, SQLConnection conn) {
        this(txId);
        this.conn = conn;
        this.isolationlevel = isolationlevel;
        clearStates();
    }

    void clearStates() {
        this.snapTxs = new ArrayList<>();
        this.snapView = new View();
        this.blocked = false;
        this.committed = false;
        this.finished = false;
        this.aborted = false;
        this.locks = new ArrayList<>();
        this.blockedStatements = new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Transaction{%d, %s}, with statements:\n", txId, isolationlevel));
        if (statements != null) {
            for (StatementCell stmt : statements) {
                sb.append("\t").append(stmt.statement).append(";\n");
            }
        }
        return sb.toString();
    }
    // 单独给Transaction中的Statements创建一个新内存备份
    public Transaction copyForStmt(){
        Transaction copy = new Transaction(txId, isolationlevel, conn);
        copy.statements = new ArrayList<StatementCell>();
        for(StatementCell cell : statements){
            StatementCell cellCopy = cell.copy();
            copy.statements.add(cellCopy);
        }
        copy.snapTxs = snapTxs;
        copy.snapView = snapView;
        copy.blocked = blocked;
        copy.committed = committed;
        copy.finished = finished;
        copy.aborted = aborted;
        copy.locks = locks;
        copy.blockedStatements = blockedStatements;
        return copy;
    }
}
