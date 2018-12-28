
{-# LANGUAGE DeriveGeneric #-}
module SlowQL.Record.Relation where
    -- Name-unrelated relational algebra tree
    import SlowQL.Record.DataType as DT
    import Data.Array.IArray
    import Data.Conduit
    import qualified SlowQL.Manage.Table as T
    import qualified SlowQL.Manage.Index as I
    import qualified SlowQL.Conduit.Combinator as Comb
    import Control.Monad
    import Conduit
    import qualified Control.Exception as Ex
    import GHC.Generics (Generic)
    import Control.DeepSeq
    newtype RecordPred=RecordPred {getPred::DT.Record->Bool} deriving (Generic)
    data RelExpr= 
                 RelEnumAll Int --table_placeholder
                | RelEnumRange Int [Int] (Maybe (Bool, [TValue])) (Maybe (Bool, [TValue])) -- index_placeholder index_columns lower_bound upper_bound
                | RelCartForeign RelExpr Int Int Int --source index_placeholder source_id placeholder_id
                | RelProjection [Int] RelExpr --columns expr
                | RelCart RelExpr RelExpr -- expr1 expr2
                | RelFilter RecordPred RelExpr 
                | RelSkip Int RelExpr-- skip
                | RelTake Int RelExpr-- take
                deriving (Show, Generic)
    instance NFData RelExpr
    instance NFData RecordPred 
    instance Show RecordPred where
        show x="[Some filter function]"
    -- evaluate (RelCartForeign source index_placeholder source_id placeholder_id) l1 l2
    evaluate :: RelExpr->Array Int T.Table->Array Int I.Index->ConduitT () DT.Record IO ()
    evaluate expr l1 l2=go expr
        where 
            go (RelEnumAll idx_table)=T.enumerateAll (l1!idx_table)
            go (RelProjection list source)=(go source) .| (Comb.projection list)
            go (RelCartForeign source index_placeholder source_col placeholder_col)=
                let lefthand=go source
                    righthand=(\lh->let current_val=(DT.fields lh)!source_col in go $ RelEnumRange index_placeholder [placeholder_col] (Just (True, [current_val])) (Just (True, [current_val])))
                    pair=Comb.concatMapC lefthand righthand
                in pair.| Comb.concatC
            go (RelCart lhs rhs)=(Comb.cartesian (go lhs) (go rhs)) .| Comb.concatC
            go (RelFilter pred expr)=(go expr).|filterC (getPred pred)
            go (RelSkip nskip expr)=(go expr) .| Comb.dropConduit nskip
            go (RelTake ntake expr)=(go expr) .| takeC ntake
    data CompiledWhereClause=
                SingleTableWhereClause Int (DT.Record->Bool) --So no need to participate in cross product.
            |   GeneralWhereClause (DT.Record->Bool) --Must be handled in total field
            |   ForeignReference Int Int Int Int --Lead to optimization into RelCartForeign
            |   WhereNotCompatiblePP Int Int Int Int --Throws an error
            |   WhereNotCompatiblePV Int Int --Throws an error 
            deriving (Generic)
    instance NFData CompiledWhereClause
    assertCompileError :: CompiledWhereClause->IO ()
    assertCompileError (WhereNotCompatiblePP a b c d)=Ex.evaluate $ error ("Field("++show a++", "++show b++") and Field("++show c++", "++show d++") not compatible!")
    assertCompileError (WhereNotCompatiblePV a b)=Ex.evaluate $ error ("Field("++show a++", "++show b++") and its value not compatible!")
    assertCompileError _=return ()
