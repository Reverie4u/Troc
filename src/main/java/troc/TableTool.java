package troc;

import lombok.extern.slf4j.Slf4j;
import troc.common.Table;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TableTool {

    static public final String RowIdColName = "rid";
    static private final String TrocTablePrefix = "_troc_";
    static private final String BackupName = "backup";
    static private final String OriginalName = "origin";
    static public final int TxSizeMin = 3;
    static public final int TxSizeMax = 6;
    static public final int CheckSize = 10;
    static public final Transaction txInit = new Transaction(0);
    static public final Randomly rand = new Randomly();
    static public final BugReport bugReport = new BugReport();
    static public int txPair = 0;
    static public int conflictTxPair = 0;
    static public boolean txPairHasConflict = false;
    static public int allCase = 0;
    static public int conflictCase = 0;;
    static public int skipCase = 0;

    static public List<IsolationLevel> possibleIsolationLevels;
    static public SQLConnection conn;
    static public Options options;
    static public String TableName = "troc"; // 该表名可以由用户指定
    static public String DatabaseName = "test";
    static public DBMS dbms;
    static int ColCount;
    static int rowIdColIdx;
    static ArrayList<String> colNames;
    static ArrayList<String> colTypeNames;
    static HashMap<String, Index> indexes;
    static String insertPrefix;
    static int nextRowId;
    static Transaction firstTxnInSerOrder;

    static void initialize(Options options) {
        dbms = DBMS.valueOf(options.getDBMS());
        TableName = options.getTableName();
        DatabaseName = options.getDbName();
        TableTool.options = options;
        TableTool.conn = getConnectionFromOptions(options);

        possibleIsolationLevels = new ArrayList<>(
                Arrays.asList(IsolationLevel.READ_COMMITTED, IsolationLevel.REPEATABLE_READ));
        if (TableTool.dbms == DBMS.MYSQL || TableTool.dbms == DBMS.MARIADB) {
            possibleIsolationLevels.add(IsolationLevel.READ_UNCOMMITTED);
            possibleIsolationLevels.add(IsolationLevel.SERIALIZABLE);
        }
    }
    /**
     * 创建JDBC连接
     * @param options
     * @return
     */
    static SQLConnection getConnectionFromOptions(Options options) {
        Connection con;
        try {
            String url = String.format("jdbc:%s://%s:%d/", dbms.getProtocol(), options.getHost(),
                    options.getPort());
            con = DriverManager.getConnection(url, options.getUserName(), options.getPassword());
            Statement statement = con.createStatement();
            statement.execute("DROP DATABASE IF EXISTS " + options.getDbName());
            statement.execute("CREATE DATABASE " + options.getDbName());
            statement.close();
            con.close();
            con = DriverManager.getConnection(url + options.getDbName(), options.getUserName(), options.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to database: ", e);
        }
        return new SQLConnection(con);
    }

    static void prepareTableFromScanner(Scanner input) {
        // 删除掉troc表，如果存在的话
        TableTool.executeOnTable("DROP TABLE IF EXISTS " + TableName);
        String sql;
        do {
            sql = input.nextLine();
            if (sql.equals("")) break;
            TableTool.executeOnTable(sql);
        } while (true);
    }

    /** 
     * 从输入流中读取事务
     */
    static Transaction readTransactionFromScanner(Scanner input, int txId) {
        Transaction tx = new Transaction(txId);
        tx.conn = genConnection();
        String isolationAlias = input.nextLine();
        tx.isolationlevel = IsolationLevel.getFromAlias(isolationAlias);
        String sql;
        int cnt = 0;
        do {
            if (!input.hasNext()) break;
            sql = input.nextLine();
            if (sql.equals("") || sql.equals("END")) break;
            tx.statements.add(new StatementCell(tx, cnt++, sql));
        } while (true);
        return tx;
    }

    /**
     * 从输入流中读取提交顺序
     * @param input
     * @return
     */
    static String readScheduleFromScanner(Scanner input) {
        do {
            if (!input.hasNext()) break;
            String scheduleStr = input.nextLine();
            if (scheduleStr.equals("")) continue;
            if (scheduleStr.equals("END")) break;
            return scheduleStr;
        } while (true);
        return "";
    }

    /**
     * 对表进行预处理
     */
    public static void preProcessTable() {
        addRowIdColumnAndFill();
        backupOriginalTable();
        fillTableMetaData();
    }

    /**
     * 为表添加rid列
     */
    private static void addRowIdColumnAndFill() {
        AtomicBoolean hasRowIdCol = new AtomicBoolean(false);
        String query = "SELECT * FROM " + TableName;
        TableTool.executeQueryWithCallback(query, rs -> {
            try {
                final ResultSetMetaData metaData = rs.getMetaData();
                final int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    if (metaData.getColumnName(i).equals(RowIdColName)) {
                        hasRowIdCol.set(true);
                        break;
                    }
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException("Add RowId column failed: ", e);
            }
        });
        if (!hasRowIdCol.get()) {
            String sql = "ALTER TABLE " + TableName + " ADD COLUMN " + RowIdColName + " INT";
            TableTool.executeOnTable(sql);
        }
        nextRowId = 1;
        fillAllRowId();
    }

    /**
     * 将数据库表的元数据提取出来
     */
    private static void fillTableMetaData() {
        nextRowId = getMaxRowId() + 1;
        colNames = new ArrayList<>();
        colTypeNames = new ArrayList<>();
        indexes = new HashMap<>();
        Statement statement;
        ResultSet rs;
        try {
            String query = "SELECT * FROM " + TableName;
            statement = conn.createStatement();
            rs = statement.executeQuery(query);
            ResultSetMetaData metaData = rs.getMetaData();
            ColCount = metaData.getColumnCount();
            for (int i = 1; i <= ColCount; i++) {
                String colName = metaData.getColumnName(i);
                if (colName.equals(RowIdColName)) {
                    rowIdColIdx = i;
                } else {
                    colNames.add(colName);
                    colTypeNames.add(metaData.getColumnTypeName(i));
                }
            }
            statement.close();
            rs.close();
            String ignore = "";
            if (dbms.getProtocol().equals("mysql")) {
                ignore = "IGNORE ";
            }
            insertPrefix = "INSERT " + ignore + "INTO " + TableName + "(" + RowIdColName + ", "
                    + String.join(", ", colNames) + ") VALUES ";
            query = String.format("select * from information_schema.statistics " +
                    "where table_schema = '%s' and table_name = '%s'", DatabaseName, TableName);
            statement = conn.createStatement();
            rs = statement.executeQuery(query);
            while (rs.next()) {
                String index_name = rs.getString("INDEX_NAME");
                if (!indexes.containsKey(index_name)) {
                    boolean isPrimary = index_name.equals("PRIMARY");
                    boolean isUnique = rs.getInt("NON_UNIQUE") == 0;
                    indexes.put(index_name, new Index(index_name, isPrimary, isUnique));
                }
                indexes.get(index_name).indexedCols.add(rs.getString("COLUMN_NAME"));
            }
            statement.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException("Fetch metadata of table failed:", e);
        }
    }
    /**
     * 制造冲突，让两个事务尝试修改同一行数据
     * @param tx1
     * @param tx2
     */
    static void makeConflict(Transaction tx1, Transaction tx2, Table table) {
        StatementCell stmt1 = randomStmtWithCondition(tx1);
        StatementCell stmt2 = randomStmtWithCondition(tx2);
        // n有问题
        // int n = getNewRowId(); // 当前表的最大rowId+1
        int n = table.getInitRowCount()+1;
        if (Randomly.getBoolean() || n == 0) {
            // Randomly.getBoolean()有50%概率为true
            // 将一条语句的where条件变成和另一条语句一样
            stmt1.whereClause = stmt2.whereClause;
            stmt1.recomputeStatement();
        } else {
            // 随机选取一行数据，确保两个事务都能修改到
            int rowId = Randomly.getNextInt(1, n);
            try {
                stmt1.makeChooseRow(rowId);
                stmt2.makeChooseRow(rowId);
            } catch (Exception e) {}
        }
    }

    static private StatementCell randomStmtWithCondition(Transaction tx) {
        ArrayList<StatementCell> candidates = new ArrayList<>();
        for (StatementCell stmt : tx.statements) {
            if (stmt.type == StatementType.UPDATE || stmt.type == StatementType.DELETE
                    || stmt.type == StatementType.SELECT || stmt.type == StatementType.SELECT_SHARE
                    || stmt.type == StatementType.SELECT_UPDATE) {
                candidates.add(stmt);
            }
        }
        return Randomly.fromList(candidates);
    }

    /**
     * 初始化外部版本链
     * @return
     */
    static HashMap<Integer, ArrayList<Version>> initVersionData() {
        // 首先用表数据初始化外部版本链
        HashMap<Integer, ArrayList<Version>> vData = new HashMap<>();
        String query = "SELECT * FROM " + TableName;
        TableTool.executeQueryWithCallback(query, rs -> {
            try {
                while (rs.next()) {
                    int rowId = 0;
                    Object[] data = new Object[ColCount - 1];
                    int idx = 0;
                    for (int i = 1; i <= ColCount; i++) {
                        if (i == rowIdColIdx) {
                            rowId = rs.getInt(i);
                        } else {
                            data[idx++] = rs.getObject(i);
                        }
                    }
                    ArrayList<Version> versions = new ArrayList<>();
                    // versions代表一个版本链
                    versions.add(new Version(data, txInit, false));
                    vData.put(rowId, versions);
                }
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        return vData;
    }

    static Lock getLock(StatementCell stmt) {
        Lock lock = new Lock();
        lock.tx = stmt.tx;
        lock.stmt = stmt;
        lock.type = LockType.NONE;
        if (stmt.type == StatementType.SELECT) {
            if (stmt.tx.isolationlevel == IsolationLevel.SERIALIZABLE) {
                lock.type = LockType.SHARE;
            }
        } else if (stmt.type == StatementType.SELECT_SHARE) {
            lock.type = LockType.SHARE;
        } else if (stmt.type == StatementType.SELECT_UPDATE
                || stmt.type == StatementType.UPDATE || stmt.type == StatementType.DELETE
                || stmt.type == StatementType.INSERT) {
            lock.type = LockType.EXCLUSIVE;
        }
        if (lock.type == LockType.NONE && stmt.type != StatementType.SELECT) return lock;
        lock.lockObject = getLockObjectFromStmtView(stmt);
        return lock;
    }

    static LockObject getLockObjectFromStmtView(StatementCell stmt) {
        String snapshotName = "get_lock";
        TableTool.takeSnapshotForTable(snapshotName);
        TableTool.viewToTable(stmt.view);
        LockObject lockObject = getLockObject(stmt);
        TableTool.recoverTableFromSnapshot(snapshotName);
        return lockObject;
    }

    static LockObject getLockObject(StatementCell stmt) {
        LockObject lockObject = new LockObject();
        HashSet<Integer> lockedRowIds = new HashSet<>();
        HashSet<String> lockedKeys = new HashSet<>();
        if (stmt.type == StatementType.INSERT) {
            lockedKeys = getIndexObjs(stmt.values);
        } else {
            HashSet<String> indexObjs = new HashSet<>();
            String query = "SELECT * FROM " + TableName + " WHERE " + stmt.whereClause;
            executeQueryWithCallback(query, rs ->{
                try {
                    while (rs.next()) {
                        lockedRowIds.add(rs.getInt(RowIdColName));
                        HashMap<String, String> rowValues = new HashMap<>();
                        for (String colName : colNames) {
                            Object obj = rs.getObject(colName);
                            if (obj != null) {
                                if (obj instanceof byte[]) {
                                    rowValues.put(colName, byteArrToHexStr((byte[]) obj));
                                } else {
                                    rowValues.put(colName, obj.toString());
                                }
                            }
                        }
                        indexObjs.addAll(getIndexObjs(rowValues));
                        if (stmt.type == StatementType.UPDATE) {
                            rowValues.putAll(stmt.values);
                            indexObjs.addAll(getIndexObjs(rowValues));
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Get lock object failed: ", e);
                }
            });
            lockedKeys.addAll(indexObjs);
        }
        lockObject.rowIds = lockedRowIds;
        lockObject.indexes = lockedKeys;
        return lockObject;
    }

    static HashSet<String> getIndexObjs(HashMap<String, String> values) {
        HashSet<String> res = new HashSet<>();
        outer:
        for (String indexName : indexes.keySet()) {
            Index index = indexes.get(indexName);
            StringBuilder sb = new StringBuilder(indexName).append(":");
            for (String colName : index.indexedCols) {
                if (!values.containsKey(colName)) {
                    continue outer;
                }
                sb.append(values.get(colName)).append(",");
            }
            res.add(sb.toString());
        }
        return res;
    }

    // 将troc表的数据转化成内存中的view
    static View tableToView() {
        View view = new View();
        String query = "SELECT * FROM " + TableName;
        TableTool.executeQueryWithCallback(query, rs -> {
            try {
                while (rs.next()) {
                    // 一行数据
                    int rowId = 0;
                    Object[] data = new Object[ColCount - 1];
                    int idx = 0;
                    for (int i = 1; i <= ColCount; i++) {
                        if (i == rowIdColIdx) {
                            rowId = rs.getInt(i);
                        } else {
                            data[idx++] = rs.getObject(i);
                        }
                    }
                    view.data.put(rowId, data);
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException("Table to view failed: ", e);
            }
        });
        return view;
    }
    // 将内存中的view覆盖到troc表
    static void viewToTable(View view) {
        clearTable(TableName);
        ArrayList<String> insertStatements = new ArrayList<>();
        int colCnt = colNames.size();
        for (int rowId : view.data.keySet()) {
            Object[] row = view.data.get(rowId);
            ArrayList<String> valStrings = new ArrayList<>();
            valStrings.add(Integer.toString(rowId));
            for (int i = 0; i < colCnt; i++) {
                valStrings.add(getValueString(row[i], colTypeNames.get(i)));
            }
            String insertSQL = insertPrefix + "(" + String.join(", ", valStrings) + ")";
            insertStatements.add(insertSQL);
        }
        for (String sql : insertStatements) {
            executeOnTable(sql);
        }
    }

    static String getValueString(Object val, String type) {
        if (val == null) return "NULL";
        switch (type.toUpperCase()) {
            case "INTEGER":
            case "INT4":
                return Integer.toString((int) val);
            case "FLOAT":
            case "FLOAT4":
                return Float.toString((float) val);
            case "DOUBLE":
            case "DECIMAL":
            case "NUMERIC":
                return Double.toString((double) val);
            case "BLOB":
                byte[] bytes = (byte[]) val;
                if (bytes.length == 0) return "NULL";
                return byteArrToHexStr(bytes);
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            default:
                return "'" + val.toString() + "'";
        }
    }

    static void clearTable(String tableName) {
        executeOnTable(String.format("DELETE FROM %s", tableName));
    }
    // 从troc表创建快照_troc_snapshotName
    static void takeSnapshotForTable(String snapshotName) {
        String trocTableName = TrocTablePrefix + snapshotName;
        cloneTable(TableName, trocTableName);
    } 
    // 从_troc_snapshotName表恢复数据到troc表
    static void recoverTableFromSnapshot(String snapshotName) {
        String trocTableName = TrocTablePrefix + snapshotName;
        cloneTable(trocTableName, TableName);
    }

    // 从troc表创建快照_troc_backup
    static void backupCurTable() {
        takeSnapshotForTable(BackupName);
    }

    // 从_troc_backup表恢复数据到troc表
    static void recoverCurTable() {
        recoverTableFromSnapshot(BackupName);
    }

    //从troc表创建快照_troc_origin
    static void backupOriginalTable() {
        takeSnapshotForTable(OriginalName);
    }

    // 从_troc_origin表恢复数据到troc表
    static void recoverOriginalTable() {
        recoverTableFromSnapshot(OriginalName);
    }

    static void cloneTable(String tableName, String newTableName) {
        try {
            Statement statement = conn.createStatement();
            statement.execute(String.format("DROP TABLE IF EXISTS %s", newTableName));
            statement.close();
            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(String.format("SHOW CREATE TABLE %s", tableName));
            rs.next();
            String createSQL = rs.getString("Create Table");
            rs.close();
            statement.close();
            createSQL = createSQL.replace("\n", "").
                    replace("CREATE TABLE `" + tableName + "`", "CREATE TABLE `" + newTableName + "`");
            statement = conn.createStatement();
            statement.execute(createSQL);
            statement.close();
            statement = conn.createStatement();
            statement.execute(String.format("INSERT INTO %s SELECT * FROM %s", newTableName, tableName));
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void executeQueryWithCallback(String query, ResultSetHandler handler) {
        Statement statement;
        ResultSet resultSet;
        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(query);
            // handle是callback函数，用于处理查询结果
            handler.handle(resultSet);
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            log.info("Execute query failed: {}", query);
            e.printStackTrace();
        }
    }
    public static int executeQueryReturnInteger(String query){
        Statement statement;
        ResultSet resultSet;
        int num = 0;
        try {
            statement = conn.createStatement();
            resultSet = statement.executeQuery(query);
            if(resultSet.next()){
                num =((Long) resultSet.getObject(1)).intValue();
            }
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            log.info("Execute query failed: {}", query);
            e.printStackTrace();
        }
        return num;
    }

    static ArrayList<Object> getFinalStateAsList() {
        return getQueryResultAsList("SELECT * FROM " + TableName);
    }

    static ArrayList<Object> getQueryResultAsList(String query) {
        return getQueryResultAsList(conn, query);
    }

    static ArrayList<Object> getQueryResultAsListWithException(SQLConnection conn, String query) throws SQLException {
        ArrayList<Object> res = new ArrayList<>();
        int columns;
        int rowIdIdx = 0;
        ResultSet rs = conn.createStatement().executeQuery(query);
        ResultSetMetaData metaData = rs.getMetaData();
        columns = metaData.getColumnCount();
        for (int i = 1; i <= columns; i++) {
            String colName = metaData.getColumnName(i);
            if (colName.equals(TableTool.RowIdColName)) {
                rowIdIdx = i;
            }
        }
        while (rs.next()) {
            int rid = 0;
            for (int i = 1; i <= columns; i++) {
                if (i == rowIdIdx) {
                    rid = rs.getInt(i);
                } else {
                    Object cell = rs.getObject(i);
                    if (cell instanceof byte[]) {
                        cell = byteArrToHexStr((byte[]) cell);
                    }
                    res.add(cell);
                }
            }
        }
        rs.close();
        return res;
    }

    static ArrayList<Object> getQueryResultAsList(SQLConnection conn, String query) {
        ArrayList<Object> res;
        try {
            res = getQueryResultAsListWithException(conn, query);
        } catch (SQLException e) {
            log.info(" -- get query result SQL exception: " + e.getMessage());
            res = new ArrayList<>();
        }
        return res;
    }

    public static boolean executeWithConn(SQLConnection conn, String sql) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.execute(sql);
            statement.close();
        } catch (SQLException e) {
            log.info("Execute SQL failed: {}", sql);
            log.info(e.getMessage());
            return false;
        }
        return true;
    }

    public static boolean executeOnTable(String sql) {
        return executeWithConn(conn, sql);
    }

    static HashSet<Integer> getRowIdsFromWhere(String whereClause) {
        HashSet<Integer> res = new HashSet<>();
        String query = "SELECT " + RowIdColName + " FROM " + TableName + " WHERE " + whereClause;
        TableTool.executeQueryWithCallback(query, rs -> {
            try {
                while (rs.next()) {
                    res.add(rs.getInt(RowIdColName));
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException("Get affected rows from where failed: ", e);
            }
        });
        return res;
    }

    static int getNewRowId() {
        return nextRowId++;
    }

    static int getMaxRowId() {
        HashSet<Integer> rowIds = getRowIdsFromWhere("true");
        int maxRowId = 0;
        for (int rowId : rowIds) {
            if (rowId > maxRowId) {
                maxRowId = rowId;
            }
        }
        return maxRowId;
    }

    static int fillOneRowId() {
        int rowId = getNewRowId();
        String sql = String.format("UPDATE %s SET %s = %d WHERE %s IS NULL LIMIT 1",
                TableName, RowIdColName, rowId, RowIdColName);
        Statement statement;
        int ret = -1;
        try {
            statement = conn.createStatement();
            ret = statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (ret == 1) {
            return rowId;
        } else if (ret == 0) {
            return -1;
        } else {
            throw new RuntimeException("Insert more than one row?");
        }
    }

    // 将所有rowId补全
    static ArrayList<Integer> fillAllRowId() {
        ArrayList<Integer> filledRowIds = new ArrayList<>();
        while (true){
            int rowId = fillOneRowId();
            if (rowId < 0) break;
            filledRowIds.add(rowId);
        }
        return filledRowIds;
    }

    public static SQLConnection genConnection() {
        Connection con;
        try {
            con = DriverManager.getConnection(conn.getConnectionURL(), options.getUserName(),
                    options.getPassword());
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to database: ", e);
        }
        return new SQLConnection(con);
    }

    public static void setIsolationLevel(Transaction tx) {
        String sql = "SET SESSION TRANSACTION ISOLATION LEVEL " + tx.isolationlevel.getName();
        TableTool.executeWithConn(tx.conn, sql);
    }

    public static boolean checkSyntax(String query) {
        try {
            Statement statement = conn.createStatement();
            statement.execute(query);
            statement.close();
        } catch (SQLException e) {
            // log.info("===Check Syntax Error: {}-{}", query, e.getMessage());
            return false;
        }
        return true;
    }

    public static void cleanTrocTables() {
        String query = "SHOW TABLES";
        executeQueryWithCallback(query, rs -> {
            try {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    if(tableName.startsWith(TrocTablePrefix)) {
                        executeOnTable("DROP TABLE " + tableName);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to clean troc tables: ", e);
            }
        });
    }

    static String byteArrToHexStr(byte[] bytes) {
        if (bytes.length == 0) {
            return "0";
        }
        final String HEX = "0123456789ABCDEF";
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        sb.append("0x");
        for (byte b : bytes) {
            sb.append(HEX.charAt((b >> 4) & 0x0F));
            sb.append(HEX.charAt(b & 0x0F));
        }
        return sb.toString();
    }
}
