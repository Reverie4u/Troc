package troc.mysql.ast;

import troc.Randomly;

public abstract class MySQLConstant implements MySQLExpression {


    public static class MySQLDoubleConstant extends MySQLConstant {

        private final double val;

        public MySQLDoubleConstant(double val) {
            this.val =val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

    }

    public static class MySQLStringConstant extends MySQLConstant {

        private final String value;
        private final boolean singleQuotes;

        public MySQLStringConstant(String value) {
            this.value = value;
            singleQuotes = Randomly.getBoolean();

        }
        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

    }

    public static class MySQLIntConstant extends MySQLConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;
        public MySQLIntConstant(long value){
            this(value, true);
        }

        public MySQLIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                stringRepresentation = String.valueOf(value);
            } else {
                stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public MySQLIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            isSigned = true;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

    }

    public static class MySQLNullConstant extends MySQLConstant {
        public MySQLNullConstant(){

        }
        @Override
        public String getTextRepresentation() {
            return "NULL";
        }
    }

    public abstract String getTextRepresentation();


    @Override
    public String toString() {
        return getTextRepresentation();
    }

}
