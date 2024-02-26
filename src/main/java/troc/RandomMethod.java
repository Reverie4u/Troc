package troc;

import java.util.HashMap;
import java.util.Random;
public class RandomMethod implements TypeMethod{
    protected HashMap<Integer,String> allInitTypes; // 初始化语句类型
    protected HashMap<Integer,String> allTxTypes;   // 事务语句类型

    public RandomMethod(){
        allInitTypes = new HashMap<>();
        allTxTypes   = new HashMap<>();
        String[] typeForInit = {"INSERT INTO","INSERT IGNORE INTO","CREATE INDEX","CREATE UNIQUE INDEX"};
        for(int i=1;i<=4;i++){
         String initType = typeForInit[i-1];
         this.allInitTypes.put(i,initType);
        }
        String[] typeFortx = {"SELECT","SELECT_SHARE","SELECT_UPDATE","UPDATE","DELETE","INSERT","BEGIN","COMMIT","ROLLBACK"};
        for(int i=1;i<=6;i++){
         String txType = typeFortx[i-1];
         this.allTxTypes.put(i,txType);
        }
    }
    
    public String selectMethodForInit(){
        Random rand = new Random();
        Integer idx = rand.nextInt(4)+1; 
        return allInitTypes.get(idx);
    }

    public String selectMethodForTx() {
        Random rand = new Random();
        Integer idx = rand.nextInt(6)+1;    
        return allTxTypes.get(idx);
    }
}
