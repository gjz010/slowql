module SlowQL.SQL.Runtime where
    import qualified SlowQL.SQL.Parser as P
    import qualified SlowQL.Record.DataType as DT
    import qualified Data.ByteString.Char8 as BC
    import Data.Array.IArray
    findParam :: [(String, DT.TParam)]->P.Column->Int
    findParam columns col=
        let icols=zip ([0..]::[Int]) columns
            match (P.LocalCol lc) (_, (a, p))=(BC.unpack $ DT.name $ DT.general  p)==lc
            match (P.ForeignCol tb fc) (_, (a,p))=(BC.unpack $ DT.name $ DT.general p)==fc && a==tb
            matched_cols=filter (match col) icols
        in if length matched_cols==1
                then fst $ head matched_cols
                else error "None or multiple field match in where clause!"
    
    applyOp :: P.Op->DT.TValue->DT.TValue->Bool
    applyOp P.OpEq=(==)
    applyOp P.OpNeq=(/=)
    applyOp P.OpLeq=(<=)
    applyOp P.OpGeq=(>=)
    applyOp P.OpLt=(<)
    applyOp P.OpGt=(>)
    compileWhere :: [(String, DT.TParam)]->P.WhereClause->DT.Record->Bool
    compileWhere _ P.WhereAny=const True
    compileWhere dom (P.WhereAnd p1 p2)=(\r->(compileWhere dom p1 r) && (compileWhere dom p2 r))
    compileWhere dom (P.WhereIsNull c isnull)=let idx=findParam dom c in (\r->(DT.isNull ((DT.fields r)!idx))==isnull)
    compileWhere dom (P.WhereOp c op (P.ExprV v))=let i=findParam dom c in (\r->applyOp op ((DT.fields r)!i) v)
    compileWhere dom (P.WhereOp c op (P.ExprC c2))=let f=findParam dom
                                                       i1=f c
                                                       i2=f c2
                                                   in (\r->(applyOp op ((DT.fields r)!i1) ((DT.fields r)!i2)))


