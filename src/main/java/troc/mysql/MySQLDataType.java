package troc.mysql;

import troc.Randomly;
import troc.common.DataType;

import java.util.Arrays;

public enum MySQLDataType implements DataType {
    TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT,

    FLOAT, DOUBLE, DECIMAL,

    BINARY, VARBINARY,

    CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT,

    BLOB, MEDIUMBLOB, LONGBLOB,

    DATE, TIME, DATETIME, TIMESTAMP,

    JSON, ENUM, SET;

    public static MySQLDataType getRandomDataType() {
        return Randomly.fromOptions(INT, FLOAT, DOUBLE, CHAR, VARCHAR, TEXT);
    }

    @Override
    public boolean isNumeric() {
        return Arrays.asList(TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT,
                FLOAT, DOUBLE, DECIMAL).contains(this);
    }

    @Override
    public boolean isString() {
        return Arrays.asList(CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT).contains(this);
    }

    @Override
    public boolean hasLen() {
        return this == CHAR || this == VARCHAR;
    }
}
