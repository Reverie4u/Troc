package troc;

import java.util.ArrayList;

public class TxnPairResult {
    private ArrayList<StatementCell> order;
    private ArrayList<Object> finalState;
    private boolean isDeadBlock;
    private boolean isSematicError;

    public TxnPairResult() {
    }

    public TxnPairResult(ArrayList<StatementCell> order, ArrayList<Object> finalState, boolean isDeadBlock,
            boolean isSematicError) {
        this.order = order;
        this.finalState = finalState;
        this.isDeadBlock = isDeadBlock;
        this.isSematicError = isSematicError;
    }

    public void setOrder(ArrayList<StatementCell> order) {
        this.order = order;
    }

    public ArrayList<StatementCell> getOrder() {
        return order;
    }

    public void setFinalState(ArrayList<Object> finalState) {
        this.finalState = finalState;
    }

    public ArrayList<Object> getFinalState() {
        return finalState;
    }

    public void setDeadBlock(boolean deadBlock) {
        isDeadBlock = deadBlock;
    }

    public boolean isDeadBlock() {
        return isDeadBlock;
    }

    public void setSematicError(boolean isSematicError) {
        this.isSematicError = isSematicError;
    }

    public boolean isSematicError() {
        return isSematicError;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Result:\nOrder:").append(order).append("\n");
        sb.append("Query Results:\n");
        for (StatementCell stmt : order) {
            sb.append("\t").append(stmt.tx.txId).append("-").append(stmt.statementId)
                    .append(": ").append(stmt.result).append("\n");
        }
        sb.append("FinalState: ").append(finalState).append("\n");
        sb.append("DeadBlock: ").append(isDeadBlock).append("\n");
        sb.append("SematicError: ").append(isSematicError).append("\n");
        return sb.toString();
    }
}
