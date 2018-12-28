{
-- SlowQL SQL Parser
module SlowQL.SQL.Parser where
import Data.Char
import qualified SlowQL.Record.DataType as DT
import qualified Data.ByteString.Char8 as BC
import Data.Either
import Data.Maybe
import Debug.Trace
}
%name parseTokens
%tokentype { Token }
%error { parseError }

%token
    create  {TokenCreate}
    drop    {TokenDrop}
    select  {TokenSelect}
    delete  {TokenDelete}
    insert  {TokenInsert}
    update  {TokenUpdate}
    from    {TokenFrom}
    where   {TokenWhere}
    ';' {TokenSemicolon}
    ',' {TokenComma}
    IDENTIFIER {TokenIdentifier $$}
    STRING {TokenString $$}
    INT {TokenInteger $$}
    FLOAT {TokenFloatLit $$}
    database    {TokenDatabase}
    table   {TokenTable}
    index   {TokenIndex}
    use {TokenUse}
    '(' {TokenOB}
    ')' {TokenCB}
    show {TokenShow}
    databases {TokenDatabases}
    int {TokenInt}
    char {TokenChar}
    float {TokenFloat}
    date {TokenDate}
    primary {TokenPrimary}
    key {TokenKey}
    not {TokenNot}
    null {TokenNull}
    tables {TokenTables}
    into {TokenInto}
    values {TokenValues}
    set {TokenSet}
    is {TokenIs}
    desc {TokenDesc}
    asc {TokenAsc}
    and {TokenAnd}
    foreign {TokenForeign}
    references {TokenReferences}
    offset {TokenOffset}
    limit {TokenLimit}
    '=' {TokenEq}
    '<>' {TokenNeq}
    '<=' {TokenLeq}
    '>=' {TokenGeq}
    '<' {TokenLt}
    '>' {TokenGt}
    '.' {TokenDot}
    '*' {TokenStar}
%%
SQL : SQLStatement ';' {$1}
SQLStatement    : CreateDatabase    {$1}
                | DropDatabase  {$1}
                | CreateTable   {$1}
                | DropTable {$1}
                | UseDatabase {$1}
                | ShowDatabases {$1}
                | ShowDatabase {$1}
                | SelectStmt {$1}
                | UpdateStmt {$1}
                | InsertStmt {$1}
                | DeleteStmt {$1}
                | CreateIndex {$1}
                | DropIndex {$1}
                | ShowTables {$1}
                | DescribeTable {$1}
CreateDatabase : create database IDENTIFIER {CreateDatabase $3}
DropDatabase   : drop database IDENTIFIER {DropDatabase $3}
CreateTable    : create table IDENTIFIER '('  DomainList ')' {CreateTable $3 $5}
DomainList     : DomainDesc {$1 (TableDescription [] Nothing [])}
               | DomainDesc ',' DomainList {$1 $3}
DomainDesc     : IDENTIFIER char MaxLength Nullable    {appendParam $ DT.TVarCharParam (DT.TGeneralParam (BC.pack $1) $4) $3}
               | IDENTIFIER int MaxLength Nullable     {appendParam $ DT.TIntParam (DT.TGeneralParam (BC.pack $1) $4) $3}
               | IDENTIFIER float Nullable     {appendParam $ DT.TFloatParam (DT.TGeneralParam (BC.pack $1) $3)}
               | IDENTIFIER date Nullable      {appendParam $ DT.TDateParam (DT.TGeneralParam (BC.pack $1) $3)}
               | primary key '(' IDENTIFIER ')' {setPrimary [$4]}
               | foreign key '(' IDENTIFIER ')' references IDENTIFIER '(' IDENTIFIER ')' {appendForeign $4 $7 $9}
MaxLength      : {- empty -} {255}
               | '(' INT ')' {$2}
Nullable       : {- empty -} {True}
               | not null {False}
DropTable      : drop table IDENTIFIER {DropTable $3}
UseDatabase    : use database IDENTIFIER {UseDatabase $3}
ShowDatabases  : show databases {ShowDatabases}
ShowDatabase   : show database {ShowDatabase}
ShowTables : show tables {ShowTables}
DescribeTable  : desc IDENTIFIER   {DescribeTable $2}

InsertStmt: insert into IDENTIFIER values  RecordList {InsertStmt $3 $5}
RecordList : '(' ValueList ')' {[$2]}
            | '(' ValueList ')' ',' RecordList {$2:$5}
ValueList: Value {[$1]}
       |   Value ',' ValueList {$1:$3}
Value: STRING {DT.ValChar (Just $ BC.pack $1)}
            | INT {DT.ValInt (Just $1)}
            | null {DT.ValNull}
            | FLOAT {DT.ValFloat (Just $1)}
TableList: IDENTIFIER {[$1]}
         | IDENTIFIER ',' TableList {$1:$3}
IsNull : null  {True}
       | not null {False}
WhereOp: Column Op Expr {WhereOp $1 $2 $3}
       | Column is IsNull {WhereIsNull $1 $3}
WhereWhereClause : {- empty -} {WhereAny}
                | where WhereClause {$2}
WhereClause : WhereOp {$1}
            |WhereOp and WhereClause {WhereAnd $1 $3}
            
Column: IDENTIFIER {LocalCol $1}
      | IDENTIFIER '.' IDENTIFIER {ForeignCol $1 $3}
Expr  : Column {ExprC $1}
      | Value {ExprV $1}
Op  : '=' {OpEq}
    | '<>' {OpNeq}
    | '<=' {OpLeq}
    | '>=' {OpGeq}
    | '<' {OpLt}
    | '>' {OpGt}

SelectSuffix: limit INT {CLimit $2}
            | offset INT {COffset $2}
SelectSuffices: {- empty -} {[]}
              | SelectSuffix SelectSuffices {$1:$2}
ColumnList: Column {[$1]}
          | Column ',' ColumnList {$1:$3}
IdtList: IDENTIFIER {[$1]}
       | IDENTIFIER ',' IdtList {$1:$3} 
SelectStmt: select ColumnList from TableList WhereWhereClause SelectSuffices {SelectStmt $2 $4 $5 $6}
          | select '*' from TableList WhereWhereClause SelectSuffices {SelectAllStmt $4 $5 $6}
UpdateStmt: update IDENTIFIER set '(' SetClause ')' WhereWhereClause {UpdateStmt $2 $5 $7}
DeleteStmt: delete from IDENTIFIER WhereWhereClause {DeleteStmt $3 $4}
SetClause: Assignment {[$1]}
         | Assignment ',' SetClause {$1:$3}
Assignment: IDENTIFIER '=' Value {($1, $3)}
CreateIndex: create index IDENTIFIER '(' IDENTIFIER ')' {CreateIndex $3 $5}
DropIndex: drop index IDENTIFIER '(' IDENTIFIER ')' {DropIndex $3 $5}
{
parseError :: [Token]->a
parseError _=error "Error while parsing SQL statement."

isText :: Char->Bool
isText c = (isAlphaNum c || c=='_')

data Token
    = TokenCreate|TokenDrop|TokenSelect|TokenUpdate|TokenDelete|TokenInsert
    |TokenFrom|TokenWhere|TokenSemicolon|TokenComma
    |TokenIdentifier String|TokenString String|TokenInteger Int|TokenDatabase|TokenTable|TokenIndex|TokenOB|TokenCB|TokenUse|TokenShow|TokenDatabases
    |TokenInt|TokenChar|TokenFloat|TokenDate|TokenPrimary|TokenKey|TokenNot|TokenNull
    |TokenTables|TokenInto|TokenValues|TokenSet|TokenIs|TokenDesc|TokenAsc|TokenAnd|TokenForeign|TokenReferences
    |TokenDot|TokenEq|TokenNeq|TokenLeq|TokenGeq|TokenLt|TokenGt|TokenStar
    |TokenOrder|TokenBy|TokenLimit|TokenOffset
    |TokenFloatLit Float
    deriving(Show)
data Op=OpEq|OpNeq|OpLeq|OpGeq|OpLt|OpGt deriving (Show)
data Column=LocalCol String|ForeignCol String String deriving (Show)
data Expr=ExprV DT.TValue | ExprC Column deriving (Show)
data WhereClause=WhereOp Column Op Expr|WhereIsNull Column Bool | WhereAnd WhereClause WhereClause | WhereAny deriving (Show)

type ForeignRef=(String, (String, String)) 
data TableDescription = TableDescription {params :: [DT.TParam], primary :: Maybe [String], fkey:: [ForeignRef]} deriving (Show)
appendParam :: DT.TParam->TableDescription->TableDescription
appendParam par d=d {params=par:(params d)}
appendForeign :: String->String->String->TableDescription->TableDescription
appendForeign fld tbl col d=d{fkey=(fld, (tbl, col)):(fkey d)}
setPrimary :: [String]->TableDescription->TableDescription
setPrimary pri d=if isNothing (primary d) then d{primary=Just pri} else error "Duplicate PrimaryKey!"
data SQLStatement
    = CreateDatabase String
    | DropDatabase String
    | CreateTable String TableDescription
    | DropTable String
    | UseDatabase String
    | ShowDatabases
    | ShowDatabase
    | SelectStmt [Column] [String] WhereClause [SelectSuffix]
    | SelectAllStmt [String] WhereClause [SelectSuffix]
    | UpdateStmt String [(String, DT.TValue)] WhereClause
    | DeleteStmt String WhereClause
    | InsertStmt String [[DT.TValue]]
    | CreateIndex String String
    | DropIndex String String
    | ShowTables
    | DescribeTable String
    deriving (Show)

data SelectSuffix
    = CLimit Int
    | COffset Int
    | COrderBy [Column] Bool deriving (Show)

tokenize :: String->[Token]
tokenize []=[]
tokenize (c:cs)
    | isSpace c=tokenize cs
    | isDigit c=readNum False $ c:cs
    | c == '-' = readNum True $ cs
    | isText c=readVar $ c:cs
tokenize (',':cs)=TokenComma:tokenize cs
tokenize (';':cs)=TokenSemicolon:tokenize cs
tokenize ('(':cs)=TokenOB:tokenize cs
tokenize (')':cs)=TokenCB:tokenize cs
tokenize ('\'':cs)=let (a, b)=span (/='\'') cs in (TokenString a):tokenize (tail b)
tokenize ('`':cs)=let (a, b)=span (/='`') cs in (TokenIdentifier a):tokenize (tail b)
tokenize ('=':cs)=TokenEq:tokenize cs
tokenize ('<':'=':cs)=TokenLeq:tokenize cs
tokenize ('>':'=':cs)=TokenGeq:tokenize cs
tokenize ('<':'>':cs)=TokenNeq:tokenize cs
tokenize ('<':cs)=TokenLt:tokenize cs
tokenize ('>':cs)=TokenGt:tokenize cs
tokenize ('.':cs)=TokenDot:tokenize cs
tokenize ('*':cs)=TokenStar:tokenize cs
readVar cs=
    let (token, rest)=span isText cs
        sym=case map toLower token of
                "database"->TokenDatabase
                "databases"->TokenDatabases
                "table"->TokenTable
                "tables"->TokenTables
                "show"->TokenShow
                "create"-> TokenCreate
                "drop"-> TokenDrop
                "use"->TokenUse
                "primary"->TokenPrimary
                "key"->TokenKey
                "not"->TokenNot
                "null"->TokenNull
                "insert"->TokenInsert
                "into"->TokenInto
                "values"->TokenValues
                "delete"-> TokenDelete
                "from"->TokenFrom
                "where"->TokenWhere
                "update"-> TokenUpdate
                "set"->TokenSet
                "select"-> TokenSelect
                "is"->TokenIs
                "int"->TokenInt
                "varchar"->TokenChar
                "char"->TokenChar
                "desc"->TokenDesc
                "asc"->TokenAsc
                "index"->TokenIndex
                "and"->TokenAnd
                "date"->TokenDate
                "float"->TokenFloat
                "foreign"->TokenForeign
                "references"->TokenReferences
                "order"->TokenOrder
                "by"->TokenBy
                "limit"->TokenLimit
                "offset"->TokenOffset
                otherwise->TokenIdentifier $ map toLower token
    in sym:tokenize rest
doneg n x= if n then -x else x
readNum neg cs=let (nump, rest)=span isDigit cs
                 in if (head rest)=='.'
                        then let (nums, rest2)=span isDigit $ tail rest in (TokenFloatLit (doneg neg $ read (nump++"."++nums))):tokenize rest2
                        else (TokenInteger (doneg neg $ read nump)):tokenize rest

tracedLex str=let tokens=tokenize str in trace (show tokens) tokens
        
parseSQL=parseTokens . tokenize
}