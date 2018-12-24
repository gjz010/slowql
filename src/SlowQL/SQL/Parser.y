{
-- SlowQL SQL Parser
module SlowQL.SQL.Parser where
import Data.Char
import qualified SlowQL.Record.DataType as DT
import qualified Data.ByteString.Char8 as BC
import Data.Either
import Data.Maybe
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
    references{TokenReferences}


%%
SQL : SQLStatement ';' {$1}
SQLStatement    : CreateDatabase    {$1}
                | DropDatabase  {$1}
                | CreateTable   {$1}
                | DropTable {$1}
                | UseDatabase {$1}
                | ShowDatabases {$1}
                | ShowDatabase {$1}

CreateDatabase : create database IDENTIFIER {CreateDatabase $3}
DropDatabase   : drop database IDENTIFIER {DropDatabase $3}
CreateTable    : create table IDENTIFIER '('  DomainList ')' {CreateTable $3 $5 $6}
DomainList     : DomainDesc {$1 (DomainDescription [] Nothing)}
               | DomainDesc ',' DomainList {$1 $3}
DomainDesc     : IDENTIFIER char MaxLength Nullable    {appendParam $ DT.TVarCharParam (DT.TGeneralParam (BC.pack $1) $4) $3}
               | IDENTIFIER int MaxLength Nullable     {appendParam $ DT.TIntParam (DT.TGeneralParam (BC.pack $1) $4) $3}
               | IDENTIFIER float Nullable     {appendParam $ DT.TFloatParam (DT.TGeneralParam (BC.pack $1) $3)}
               | IDENTIFIER date Nullable      {appendParam $ DT.TFloatParam (DT.TGeneralParam (BC.pack $1) $3)}
               | primary key IDENTIFIER {setPrimary $3}
               | foreign key '(' IDENTIFIER ')' references IDENTIFIER '(' IDENTIFIER ')' {appendForeign $4 $7 $9}
MaxLength      : {- empty -} {255}
               | '(' INT ')' {$2}
Nullable       : {- empty -} {True}
               | not null {False}
DropTable      : drop table IDENTIFIER {DropTable $3}
UseDatabase    : use database IDENTIFIER {UseDatabase $3}
ShowDatabases  : show databases {ShowDatabases}
ShowDatabase   : show database {ShowDatabase}
ShowTables : show tables
DescribeTable : desc IDENTIFIER {DescribeTable desc}
InsertStmt: insert into IDENTIFIER values InsertValues {InsertStmt $3 $5}
ValueList: Value {[$1]}
                  |   Value ',' ValueList [$1:$3]
Value: TokenString {let TokenString str=$1 in DT.ValChar (Just str)}
            | TokenInt {let TokenInt i=$1 in DT.ValInt (Just i)}
            | TokenNull {DT.ValNull}
SelectStmt: select Selector from IDENTIFIER WhereClause {SelectStmt $2 }
UpdateStmt: update IDENTIFIER set SetClause where WhereClause
DeleteStmt: delete from IDENTIFIER where WhereClause

{
parseError :: [Token]->a
parseError _=error "Error while parsing SQL statement."

isText :: Char->Bool
isText c = (isAlphaNum c || c=='_' || c=='-')

data Token
    = TokenCreate|TokenDrop|TokenSelect|TokenUpdate|TokenDelete|TokenInsert
    |TokenFrom|TokenWhere|TokenSemicolon|TokenComma
    |TokenIdentifier String|TokenString String|TokenInteger Int|TokenDatabase|TokenTable|TokenIndex|TokenOB|TokenCB|TokenUse|TokenShow|TokenDatabases
    |TokenInt|TokenChar|TokenFloat|TokenDate|TokenPrimary|TokenKey|TokenNot|TokenNull
    |TokenTables|TokenInto|TokenValues|TokenSet|TokenIs|TokenDesc|TokenAsc|TokenAnd|TokenForeign|TokenReferences
    deriving(Show)
type ForeignRef=(String, String, String)
data TableDescription = TableDescription {params :: [DT.TParam], primary :: Maybe String, foreign: [ForeignRef]} 
appendParam :: DT.TParam->TableDescription->TableDescription
appendParam par d=d {params=par:(params d)}
appendForeign :: String->String->String->TableDescription->TableDescription
appendForeign fld->tbl col d=d{foreign=(fld, tbl, col):(foreign d)}
setPrimary :: String->TableDescription->TableDescription
setPrimary pri d=if isNothing (primary d) then d{primary=Just pri} else error "Duplicate PrimaryKey!"
data SQLStatement
    = CreateDatabase String
    | DropDatabase String
    | CreateTable String TableDescription
    | DropTable String
    | UseDatabase String
    | ShowDatabases
    | ShowDatabase
    deriving (Show)

tokenize :: String->[Token]
tokenize []=[]
tokenize (c:cs)
    | isSpace c=tokenize cs
    | isDigit c=readNum $ c:cs
    | isText c=readVar $ c:cs
tokenize (',':cs)=TokenComma:tokenize cs
tokenize (';':cs)=TokenSemicolon:tokenize cs
tokenize ('(':cs)=TokenOB:tokenize cs
tokenize (')':cs)=TokenCB:tokenize cs
tokenize ('\'':cs)=let (a, b)=span (/='\'') cs in (TokenString $ init a):tokenize ((last a):b)
readVar cs=
    let (token, rest)=span isText cs
        sym=case token of

                
                
                
                
                
                
                
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
                "desc"->TokenDesc
                "asc"->TokenAsc
                "index"->TokenIndex
                "and"->TokenAnd
                "date"->TokenDate
                "float"->TokenFloat
                "foreign"->TokenForeign
                "references"->TokenReferences


                
                

                otherwise->TokenIdentifier token
    in sym:tokenize rest
readNum cs=TokenInteger (read num):tokenize rest
        where (num, rest)=span isDigit cs
parseSQL=parseTokens . tokenize
}