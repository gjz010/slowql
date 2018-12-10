{
-- SlowQL SQL Parser
module SlowQL.SQL.Parser where
import Data.Char
import qualified SlowQL.Record.DataType as DT
import qualified Data.ByteString.Char8 as BC
import Data.Either
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
CreateTable    : create table IDENTIFIER '('  DomainList  PrimaryKey ')' {CreateTable $3 $5 $6}
PrimaryKey     : {- empty -} {Nothing}
               | ',' primary key IDENTIFIER {Just $4}
DomainList     : DomainDesc {[$1]}
               | DomainDesc ',' DomainList {$1:$3}
DomainDesc     : IDENTIFIER char MaxLength Nullable    {(DT.TVarCharParam (DT.TGeneralParam (BC.pack $1) $4) $3)}
               | IDENTIFIER int MaxLength Nullable     {DT.TIntParam (DT.TGeneralParam (BC.pack $1) $4) $3}
               | IDENTIFIER float Nullable     {DT.TFloatParam (DT.TGeneralParam (BC.pack $1) $3)}
               | IDENTIFIER date Nullable      {DT.TFloatParam (DT.TGeneralParam (BC.pack $1) $3)}
MaxLength      : {- empty -} {255}
               | '(' INT ')' {$2}
Nullable       : {- empty -} {True}
               | not null {False}
DropTable      : drop table IDENTIFIER {DropTable $3}
UseDatabase    : use database IDENTIFIER {UseDatabase $3}
ShowDatabases  : show databases {ShowDatabases}
ShowDatabase   : show database {ShowDatabase}
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
    deriving(Show)
data SQLStatement
    = CreateDatabase String
    | DropDatabase String
    | CreateTable String [DT.TParam] (Maybe String)
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
                "create"-> TokenCreate
                "drop"-> TokenDrop
                "select"-> TokenSelect
                "update"-> TokenUpdate
                "delete"-> TokenDelete
                "insert"->TokenInsert
                "from"->TokenFrom
                "where"->TokenWhere
                "database"->TokenDatabase
                "table"->TokenTable
                "index"->TokenIndex
                "use"->TokenUse
                "show"->TokenShow
                "databases"->TokenDatabases
                "int"->TokenInt
                "char"->TokenChar
                "float"->TokenFloat
                "date"->TokenDate
                "primary"->TokenPrimary
                "key"->TokenKey
                "not"->TokenNot
                "null"->TokenNull
                otherwise->TokenIdentifier token
    in sym:tokenize rest
readNum cs=TokenInteger (read num):tokenize rest
        where (num, rest)=span isDigit cs
parseSQL=parseTokens . tokenize
}