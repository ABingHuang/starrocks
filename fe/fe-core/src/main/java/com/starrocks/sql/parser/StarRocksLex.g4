// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

lexer grammar StarRocksLex;
@parser::members {public static long sqlMode;}
tokens {
    CONCAT
}

ADD: 'ADD';
ADMIN: 'ADMIN';
AFTER: 'AFTER';
AGGREGATE: 'AGGREGATE';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANTI: 'ANTI';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
ASYNC: 'ASYNC';
AUTHORS: 'AUTHORS';
AVG: 'AVG';
BACKEND: 'BACKEND';
BACKENDS: 'BACKENDS';
BACKUP: 'BACKUP';
BEGIN: 'BEGIN';
BETWEEN: 'BETWEEN';
BIGINT: 'BIGINT';
BITMAP: 'BITMAP';
BITMAP_UNION: 'BITMAP_UNION';
BOOLEAN: 'BOOLEAN';
BOTH: 'BOTH';
BROKER: 'BROKER';
BUCKETS: 'BUCKETS';
BUILTIN: 'BUILTIN';
BY: 'BY';
CANCEL: 'CANCEL';
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
CHAIN: 'CHAIN';
CHAR: 'CHAR';
CHARSET: 'CHARSET';
CHECK: 'CHECK';
COLLATE: 'COLLATE';
COLLATION: 'COLLATION';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONFIG: 'CONFIG';
CONNECTION: 'CONNECTION';
CONNECTION_ID: 'CONNECTION_ID';
CONSISTENT: 'CONSISTENT';
COSTS: 'COSTS';
COUNT: 'COUNT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATA: 'DATA';
DATABASE: 'DATABASE';
DATABASES: 'DATABASES';
DATE: 'DATE';
DATETIME: 'DATETIME';
DAY: 'DAY';
DECIMAL: 'DECIMAL';
DECIMALV2: 'DECIMALV2';
DECIMAL32: 'DECIMAL32';
DECIMAL64: 'DECIMAL64';
DECIMAL128: 'DECIMAL128';
DEFAULT: 'DEFAULT';
DELETE: 'DELETE';
DENSE_RANK: 'DENSE_RANK';
NTILE: 'NTILE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DISTRIBUTION: 'DISTRIBUTION';
DOUBLE: 'DOUBLE';
DROP: 'DROP';
DUAL: 'DUAL';
DUPLICATE: 'DUPLICATE';
DYNAMIC: 'DYNAMIC';
ELSE: 'ELSE';
END: 'END';
ENGINE: 'ENGINE';
ENGINES: 'ENGINES';
ERRORS: 'ERRORS';
EVENTS: 'EVENTS';
EXCEPT: 'EXCEPT';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXPORT: 'EXPORT';
EXTERNAL: 'EXTERNAL';
EXTRACT: 'EXTRACT';
EVERY: 'EVERY';
FALSE: 'FALSE';
FILE: 'FILE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FIRST_VALUE: 'FIRST_VALUE';
FLOAT: 'FLOAT';
FN: 'FN';
FOLLOWING: 'FOLLOWING';
FOLLOWER: 'FOLLOWER';
FOR: 'FOR';
FORCE: 'FORCE';
FORMAT: 'FORMAT';
FREE: 'FREE';
FROM: 'FROM';
FRONTEND: 'FRONTEND';
FRONTENDS: 'FRONTENDS';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GLOBAL: 'GLOBAL';
GRANT: 'GRANT';
GRANTS: 'GRANTS';
GROUP: 'GROUP';
GROUPS: 'GROUPS';
GROUPING: 'GROUPING';
GROUPING_ID: 'GROUPING_ID';
HASH: 'HASH';
HAVING: 'HAVING';
HELP: 'HELP';
HLL: 'HLL';
HLL_UNION: 'HLL_UNION';
HOUR: 'HOUR';
IDENTIFIED: 'IDENTIFIED';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INDEX: 'INDEX';
INDEXES: 'INDEXES';
INFILE: 'INFILE';
INSTALL: 'INSTALL';
INNER: 'INNER';
INSERT: 'INSERT';
INT: 'INT';
INTEGER: 'INTEGER';
INTERMEDIATE: 'INTERMEDIATE';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
OVERWRITE: 'OVERWRITE';
IS: 'IS';
ISOLATION: 'ISOLATION';
JOIN: 'JOIN';
JSON: 'JSON';
KEY: 'KEY';
KEYS: 'KEYS';
KILL: 'KILL';
LABEL: 'LABEL';
LAG: 'LAG';
LARGEINT: 'LARGEINT';
LAST: 'LAST';
LAST_VALUE: 'LAST_VALUE';
LATERAL: 'LATERAL';
LEAD: 'LEAD';
LEFT: 'LEFT';
LESS: 'LESS';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LIST: 'LIST';
LOAD: 'LOAD';
LOCAL: 'LOCAL';
LOGICAL: 'LOGICAL';
MANUAL: 'MANUAL';
MATERIALIZED: 'MATERIALIZED';
MAX: 'MAX';
MAXVALUE: 'MAXVALUE';
MERGE: 'MERGE';
MIN: 'MIN';
MINUTE: 'MINUTE';
MINUS: 'MINUS';
MOD: 'MOD';
MODIFY: 'MODIFY';
MONTH: 'MONTH';
NAME: 'NAME';
NAMES: 'NAMES';
NEGATIVE: 'NEGATIVE';
NO: 'NO';
NOT: 'NOT';
NULL: 'NULL';
NULLS: 'NULLS';
OBSERVER: 'OBSERVER';
OFFSET: 'OFFSET';
ON: 'ON';
ONLY: 'ONLY';
OPEN: 'OPEN';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
OUTFILE: 'OUTFILE';
OVER: 'OVER';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
PASSWORD: 'PASSWORD';
PATH: 'PATH';
PAUSE: 'PAUSE';
PERCENTILE: 'PERCENTILE';
PERCENTILE_UNION: 'PERCENTILE_UNION';
PLUGIN: 'PLUGIN';
PLUGINS: 'PLUGINS';
PRECEDING: 'PRECEDING';
PRIMARY: 'PRIMARY';
PROC: 'PROC';
PROCEDURE: 'PROCEDURE';
PROCESSLIST: 'PROCESSLIST';
PROPERTIES: 'PROPERTIES';
PROPERTY: 'PROPERTY';
QUARTER: 'QUARTER';
QUERY: 'QUERY';
QUOTA: 'QUOTA';
RANDOM: 'RANDOM';
RANGE: 'RANGE';
RANK: 'RANK';
READ: 'READ';
RECOVER: 'RECOVER';
REFRESH: 'REFRESH';
REGEXP: 'REGEXP';
RELEASE: 'RELEASE';
RENAME: 'RENAME';
REPAIR: 'REPAIR';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
REPLACE_IF_NOT_NULL: 'REPLACE_IF_NOT_NULL';
REPLICA: 'REPLICA';
REPOSITORY: 'REPOSITORY';
REPOSITORIES: 'REPOSITORIES';
RESOURCE: 'RESOURCE';
RESTORE: 'RESTORE';
RESUME: 'RESUME';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROUTINE: 'ROUTINE';
ROW: 'ROW';
ROWS: 'ROWS';
ROW_NUMBER: 'ROW_NUMBER';
SUBMIT: 'SUBMIT';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SELECT: 'SELECT';
SEMI: 'SEMI';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SET_VAR: 'SET_VAR';
SIGNED: 'SIGNED';
SHOW: 'SHOW';
SMALLINT: 'SMALLINT';
SNAPSHOT: 'SNAPSHOT';
START: 'START';
STATUS: 'STATUS';
STOP: 'STOP';
STORAGE: 'STORAGE';
STRING: 'STRING';
SUM: 'SUM';
SYNC: 'SYNC';
SYSTEM: 'SYSTEM';
SWAP: 'SWAP';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLET: 'TABLET';
TASK: 'TASK';
TEMPORARY: 'TEMPORARY';
THAN: 'THAN';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TIMESTAMPADD: 'TIMESTAMPADD';
TIMESTAMPDIFF: 'TIMESTAMPDIFF';
TINYINT: 'TINYINT';
TRANSACTION: 'TRANSACTION';
TO: 'TO';
TRIGGERS: 'TRIGGERS';
TRUE: 'TRUE';
TRUNCATE: 'TRUNCATE';
TYPE: 'TYPE';
TYPES: 'TYPES';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
UNIQUE: 'UNIQUE';
UNINSTALL: 'UNINSTALL';
UNSIGNED: 'UNSIGNED';
UPDATE: 'UPDATE';
USE: 'USE';
USER: 'USER';
USING: 'USING';
VALUE: 'VALUE';
VALUES: 'VALUES';
VARCHAR: 'VARCHAR';
VARIABLES: 'VARIABLES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
WARNINGS: 'WARNINGS';
WEEK: 'WEEK';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';
EQ_FOR_NULL: '<=>';

PLUS_SYMBOL: '+';
MINUS_SYMBOL: '-';
ASTERISK_SYMBOL: '*';
SLASH_SYMBOL: '/';
PERCENT_SYMBOL: '%';

LOGICAL_OR: '||' {setType((StarRocksParser.sqlMode & com.starrocks.qe.SqlModeHelper.MODE_PIPES_AS_CONCAT) == 0 ? LOGICAL_OR : StarRocksParser.CONCAT);};
LOGICAL_AND: '&&';
LOGICAL_NOT: '!';

INT_DIV: 'DIV';
BITAND: '&';
BITOR: '|';
BITXOR: '^';
BITNOT: '~';

ARROW: '->';
AT: '@';

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

SINGLE_QUOTED_TEXT
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

DOUBLE_QUOTED_TEXT
    : '"' ( '\\'. | '""' | ~('"'| '\\') )* '"'
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-zA-Z_$\u0080-\uffff]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' ~'+' .*? '*/' -> channel(HIDDEN)
    ;

SEMICOLON: ';';

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
