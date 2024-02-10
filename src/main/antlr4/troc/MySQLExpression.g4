grammar MySQLExpression;

expression:
	literal
	| LEFT_BRACKET expression RIGHT_BRACKET
	| expression BETWEEN expression AND expression
	| expression (NOT)? IN LEFT_BRACKET expressionList RIGHT_BRACKET
	| CAST LEFT_BRACKET expression AS TYPE RIGHT_BRACKET;
	
expressionList
    :   expression (',' expression)*
    ;


literal: INTEGER_LITERAL | SIGNED_INTEGER_LITERAL | NULL_LITERAL | COLUMN_NAME; 

SIGNED_INTEGER_LITERAL: '-' INTEGER_LITERAL;

TYPE: UNSIGNED | SIGNED;
UNSIGNED: 'UNSIGNED';
SIGNED: 'SIGNED';
CAST: 'CAST';
AS: 'AS';
NOT: 'NOT';
IN: 'IN';
BETWEEN: 'BETWEEN';
AND: 'AND';
INTEGER_LITERAL: [0-9]+;
NULL_LITERAL    : 'NULL' ;
COLUMN_NAME: [a-z0-9]+;
WS: [ \t\r\n]+ -> skip;
LEFT_BRACKET: '(';
RIGHT_BRACKET: ')';