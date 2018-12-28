module SlowQL.Conduit.Combinator where
    import Conduit
    import Data.Conduit
    import qualified SlowQL.Record.DataType as DT
    import Data.Array.IArray

    concatMapC :: (Monad m)=>(ConduitT () o1 m ())->(o1->ConduitT () o2 m ())->(ConduitT () (o1, o2) m ())
    concatMapC lc f=lc .| (awaitForever (\lh->(return () :: ConduitT o1 () m ()) .| (f lh) .| mapC (\rh->(lh, rh)))) --This took me one night to write.

    cartesian :: (Monad m)=>(ConduitT () o1 m ())->(ConduitT () o2 m ())->(ConduitT () (o1, o2) m ())
    cartesian lc rc=lc .| (awaitForever (\lh->(return () :: ConduitT o1 () m ()) .| rc .| mapC (\rh->(lh, rh)))) --This took me one night to write.

    

    concatC :: (Monad m)=>(ConduitT (DT.Record, DT.Record) DT.Record m ())
    concatC=mapC (\(r1, r2)->DT.Record $ DT.buildArray $  ((elems $ DT.fields r1)++(elems $ DT.fields r2)))
    projection :: (Monad m)=>[Int]->ConduitT DT.Record DT.Record m ()
    projection list=mapC (\r->DT.Record $ DT.buildArray $ map ((!) $ DT.fields r) list)

    dropConduit n = dropC n >> mapC id