
{-# LANGUAGE DeriveGeneric #-}
module SlowQL.Record.Relation where
    -- Name-unrelated relational algebra tree
    import SlowQL.Record.DataType as DT
    import Data.Array.IArray
    import Data.Conduit
    import Data.Tuple
    import qualified SlowQL.Manage.Table as T
    --import qualified SlowQL.Manage.Index as I
    import qualified SlowQL.Conduit.Combinator as Comb
    import Control.Monad
    import Conduit
    import qualified Control.Exception as Ex
    import GHC.Generics (Generic)
    import Control.DeepSeq
    import qualified SlowQL.Manage.Index as I
    newtype RecordPred=RecordPred {getPred::DT.Record->Bool} deriving (Generic)
    type Bound=(Maybe (Bool, DT.TValue), Maybe (Bool, DT.TValue))

    intersect :: Bound->Bound->Bound
    intersect (a1, a2) (b1, b2)=(lbound a1 b1, ubound a2 b2)
            where
                lbound Nothing r=r
                lbound l Nothing=l
                lbound (Just (l1, l2)) (Just (r1, r2))=case (compare l2 r2) of
                        LT -> Just (r1, r2)
                        EQ -> Just (l1&&r1, r2)
                        GT -> Just (l1, l2)
                ubound Nothing r=r
                ubound l Nothing=l
                ubound (Just (l1, l2)) (Just (r1, r2))=case (compare l2 r2) of
                        LT -> Just (l1, l2)
                        EQ -> Just (l1&&r1, r2)
                        GT -> Just (r1, r2)
    data RelExpr= 
                 RelEnumAll Int --table_placeholder
                | RelEnumRange Int (Maybe (Bool, DT.TValue)) (Maybe (Bool, DT.TValue)) -- index_placeholder lower_bound upper_bound
                | RelCartForeign RelExpr Int Int --source index_placeholder source_id
                | RelProjection [Int] RelExpr --columns expr
                | RelCart RelExpr RelExpr -- expr1 expr2
                | RelFilter RecordPred RelExpr 
                | RelInterval RecordPred RelExpr Int Bound
                | RelSkip Int RelExpr-- skip
                | RelTake Int RelExpr-- take
                | RelNothing
                deriving (Show, Generic)
    -- The merge randomly chooses a column with index to perform the query.
    -- It would have better be a list of filters, or something fishy may happen.
    mergeEnumRange :: Int->RelExpr->(Maybe Int, RelExpr)
    mergeEnumRange index_placeholder expr=go Nothing expr
                where
                    isRange (RelEnumRange{}) = True
                    isRange _ =False
                    go (Just (i, (a, b))) (RelEnumAll _)=(Just i, RelEnumRange index_placeholder a b) -- apply range
                    go (Nothing) (RelEnumAll x)=(Nothing, RelEnumAll x) --Do nothing.
                    go i (RelFilter a b)=let (m, n)=go i b in (m, RelFilter a n)
                    go Nothing (RelInterval a b i c )=let (r1, r2)=go (Just (i, c)) b
                                                   in (r1,  r2)
                    go (Just (i',c')) (RelInterval a b i c)=if i'==i 
                                                            then
                                                                let new_bound=intersect c c'
                                                                    (r1, r2)=go (Just (i, new_bound)) b
                                                                in (r1,  r2)
                                                            else let (m, n)=go (Just (i', c')) b in (m, RelFilter a n)
    isRelNothing RelNothing=True
    isRelNothing _=False
    instance NFData RelExpr
    instance NFData RecordPred 
    instance Show RecordPred where
        show x="[Some filter function]"
    -- evaluate (RelCartForeign source index_placeholder source_id placeholder_id) l1 l2
    evaluate :: RelExpr->Array Int T.Table->Array Int I.Index->ConduitT () DT.Record IO ()
    evaluate expr l1 l2=go expr
        where 
            go (RelEnumAll idx_table)=T.enumerateAll (l1!idx_table)
            go (RelEnumRange idx_index l u)=let index=(l2!idx_index) in I.enumerateRange index (fmap swap l) (fmap swap u)-- index_placeholder index_columns lower_bound upper_bound
            go (RelProjection list source)=(go source) .| (Comb.projection list)
            go (RelCartForeign source index_placeholder source_col )=
                let lefthand=go source
                    righthand=(\lh->let current_val=(DT.fields lh)!source_col in go $ RelEnumRange index_placeholder  (Just (True, current_val)) (Just (True, current_val)))
                    pair=Comb.concatMapC lefthand righthand
                in pair.| Comb.concatC
            go (RelCart lhs rhs)=(Comb.cartesian (go lhs) (go rhs)) .| Comb.concatC
            go (RelFilter pred expr)=(go expr).|filterC (getPred pred)
            go (RelSkip nskip expr)=(go expr) .| Comb.dropConduit nskip
            go (RelTake ntake expr)=(go expr) .| takeC ntake
    data CompiledWhereClause=
                SingleTableWhereClause Int (DT.Record->Bool) (DT.Record->Bool)--So no need to participate in cross product.
            |   GeneralWhereClause (DT.Record->Bool) --Must be handled in total field
            |   ForeignReference Int Int Int (DT.Record->Bool) --Lead to optimization into RelCartForeign
            |   WhereNotCompatiblePP Int Int Int Int --Throws an error
            |   WhereNotCompatiblePV Int Int --Throws an error 
            |   OptimizableSingleTableWhereClause Int Int (Maybe (Bool, DT.TValue)) (Maybe (Bool, DT.TValue)) (DT.Record->Bool) (DT.Record->Bool)--Lead to optimization into RelEnumRange
            deriving (Generic)
    instance Show CompiledWhereClause where
        show (SingleTableWhereClause a b c)=show ("SingleTableWhereClause", a)
        show (GeneralWhereClause _)=show ("GeneralWhereClause")
        show (ForeignReference a b c _)=show ("ForeignReference", a, b, c)
        show (OptimizableSingleTableWhereClause a b c d _ _)=show ("OptimizableSingleTableWhereClause", a, b, c, d)
    isForeignRef ForeignReference{}=True
    isForeignRef _=False
    isOptSTC OptimizableSingleTableWhereClause{}=True
    isOptSTC _=False
    isSTC SingleTableWhereClause{}=True
    isSTC _=False
    isGeneralWC GeneralWhereClause{}=True
    isGeneralWC _=False
    fallback (SingleTableWhereClause _ _ f)=f
    fallback (GeneralWhereClause f)=f
    fallback (ForeignReference _ _ _ f)=f
    fallback (OptimizableSingleTableWhereClause _ _ _ _ _ f)=f
    instance NFData CompiledWhereClause
    assertCompileError :: CompiledWhereClause->IO ()
    assertCompileError (WhereNotCompatiblePP a b c d)=Ex.evaluate $ error ("Field("++show a++", "++show b++") and Field("++show c++", "++show d++") not compatible!")
    assertCompileError (WhereNotCompatiblePV a b)=Ex.evaluate $ error ("Field("++show a++", "++show b++") and its value not compatible!")
    assertCompileError _=return ()
