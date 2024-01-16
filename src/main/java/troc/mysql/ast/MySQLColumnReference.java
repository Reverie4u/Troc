package troc.mysql.ast;

import java.util.Map;

import troc.mysql.MySQLColumn;
import troc.mysql.MySQLDataType;
import troc.mysql.ast.MySQLConstant.MySQLDoubleConstant;

public class MySQLColumnReference implements MySQLExpression {

    private final MySQLColumn column;
    private final MySQLConstant value;

    public MySQLColumnReference(MySQLColumn column, MySQLConstant value) {
        this.column = column;
        this.value = value;
    }

    public static MySQLColumnReference create(MySQLColumn column, MySQLConstant value) {
        return new MySQLColumnReference(column, value);
    }

    public MySQLColumn getColumn() {
        return column;
    }

    public MySQLConstant getValue() {
        return value;
    }

    @Override
    public MySQLConstant getExpectedValue(Map<String, Object> row) {
        // 根据列类型，构造MySQLConstant
        switch ((MySQLDataType) column.getDataType()) {
            case TINYINT:
            case SMALLINT:
            case MEDIUMINT:
            case INT:
            case BIGINT:
                Integer intVal = (Integer) row.get(column.getColumnName());
                if (intVal == null) {
                    return MySQLConstant.createNullConstant();
                }
                return MySQLConstant.createIntConstant(intVal.intValue());
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return new MySQLDoubleConstant((double) row.get(column.getColumnName()));
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                String str = (String) row.get(column.getColumnName());
                if (str == null) {
                    return MySQLConstant.createNullConstant();
                }
                return MySQLConstant.createStringConstant((String) row.get(column.getColumnName()));
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

}
