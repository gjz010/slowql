{-# LANGUAGE DeriveGeneric #-}
module SlowQL.Manage.Database where
    import qualified SlowQL.Manage.Table as T
    import qualified Data.ByteString as BS
    import qualified Data.ByteString.Lazy as BL
    import qualified SlowQL.Record.DataType as DT
    import qualified Data.ByteString.Char8 as BC
    import Data.Binary.Put
    import Data.Binary.Get
    import System.IO
    import System.Directory
    import SlowQL.Utils
    import Data.Conduit
    import Conduit
    import Data.IORef
    import qualified Data.Array.IArray as Arr
    import Data.Array.IArray ((!))
    import qualified Data.Map.Strict as Map
    import qualified SlowQL.SQL.Parser as P
    import qualified SlowQL.Record.Relation as R
    import qualified Data.Vector.Mutable as V
    import qualified SlowQL.Manage.Index as I
    import Control.Monad
    import Control.Exception
    import Data.List
    import Control.DeepSeq
    type ByteString=BS.ByteString
    data DatabaseDef=DatabaseDef {name :: !ByteString, tables :: ![T.TableDef], indices :: ![I.IndexDef]} deriving (Show)
    data Database=Database {def :: !(IORef DatabaseDef), live_tables :: !(IORef (Map.Map ByteString T.Table)), live_indices :: !(IORef (Map.Map (ByteString, Int) I.Index))}
    tablePath :: String->String->String
    tablePath base name="slowql-data/"++base++"/"++name++".table"
    indexPath :: String->I.IndexDef->String
    indexPath base def="slowql-data/"++base++"/"++(BC.unpack $ I.name def)++(show $ I.target def)++".index"
    flushDatabaseDef :: Database->IO()
    flushDatabaseDef db=do
        def<-readIORef $ def db
        BL.writeFile ("slowql-data/"++(BC.unpack $ name def)++"/.slowql") $ runPut $ serialize def
    openTable :: ByteString->T.TableDef->IO (ByteString, T.Table)
    openTable base df=do
        let path=tablePath (BC.unpack base) (BC.unpack $ T.name df)
        t<-T.open path df
        return $ seq df (T.name df, t)
    showDatabase :: Database->IO()
    showDatabase db=do
        ddf<-readIORef (def db)
        print ddf
        lt<-readIORef (live_tables db)
        print lt
        li<-readIORef (live_indices db)
        print li
    openIndex :: Database->I.IndexDef->IO ()
    openIndex db df=do
        ddf<-readIORef (def db)
        let base=name ddf
        let path=indexPath (BC.unpack base) df
        [table]<-getTablesByName [BC.unpack $ I.name df] db
        i<-I.open path df (T.def table)
        modifyIORef' (live_indices db) (Map.insert (I.name df, I.target df) i)
    openAllIndices :: Database->IO ()
    openAllIndices db=do
        ddf<-readIORef (def db)
        mapM_ (openIndex db) (indices ddf)

    openAllTables :: DatabaseDef->IO (Map.Map ByteString T.Table)
    openAllTables df=do
        l<-mapM (openTable $ name df) $ tables df
        return $ Map.fromList l
    showTables :: Database->IO ()
    showTables db=do
        d<-readIORef $ def db
        print $ map T.name $ tables d
        
    
    createTable :: Database->String->[DT.TParam]->Maybe [String]->[(String, (String, String))]->IO ()
    createTable db name orig_params primary fkey=do
        -- Pre-create check
        let bname=BC.pack name
        let params=(DT.ridParam):orig_params
        tdef<-readIORef $ def db
        let primary_pred=case primary of
                Nothing->True
                (Just jp)-> (not $ hasDuplicates jp) && ( all (\p->any (\par->BC.pack p==(DT.name $ DT.general par) ) params) jp)
        checkWhen (all (\d->T.name d/=bname) $ tables tdef) "Duplicated table name!"  $
            checkWhen primary_pred "Bad primary key name!" $
            checkWhen (all (\(p, _)->any (\par->BC.pack p==(DT.name $ DT.general par)) params) fkey) "Bad foreign key name!" $
            do
                let resolve_primary=case primary of
                        Nothing -> 0
                        (Just [jp])-> forceJust "Bad primary key!" $ findIndex (\elm->(DT.name $ DT.general elm )== BC.pack jp) params
                tbdef<-T.create (tablePath (BC.unpack $ SlowQL.Manage.Database.name tdef) name) name (DT.Domains $ DT.buildArray' params) resolve_primary
                (pa,pb)<-openTable (SlowQL.Manage.Database.name tdef) tbdef
                modifyIORef' (live_tables db) (Map.insert pa pb)
                writeIORef (def db) (tdef {tables=tbdef:(tables tdef)})
                putStrLn "CREATE TABLE"

    getAllRelatedIndices :: T.TableDef->Database->IO [I.Index]
    getAllRelatedIndices tdef db=do
        li<-readIORef $ live_indices db
        return $ snd $ unzip $ filter (\(k, a)->T.name tdef==fst k) $ Map.toList li
    createIndex :: String->String->Database->IO ()
    createIndex tblname colname db=do
        [table]<-getTablesByName [tblname] db
        ddef<-readIORef $ def db
        let (_,column_id,_,_)= forceJust "Column missing!" $ translateColumn ([T.def table]) $ P.LocalCol colname
        let idef=I.IndexDef (BC.pack tblname) column_id
        checkWhen (not $ hasDuplicates $ idef:(indices ddef)) "Duplicated Index!" $ do
            I.create (indexPath tblname idef) (T.def table) column_id
            writeIORef (def db) ddef{indices=idef:(indices ddef)}
            openIndex db idef
            putStrLn "CREATE INDEX"
    dropIndex :: String->String->Database->IO()
    dropIndex tblname colname db=do
        [table]<-getTablesByName [tblname] db
        let (_,column_id,_,_)= forceJust "Column missing!" $ translateColumn ([T.def table]) $ P.LocalCol colname
        dropIndex' (BC.pack tblname, column_id) db
        putStrLn "DROP INDEX"

    dropIndex' :: (ByteString, Int)->Database->IO()
    dropIndex' (tname, target) db=do
        li<-readIORef $ live_indices db
        let k=(tname, target)
        I.close ((Map.!) li k)
        --print "Drop!"
        modifyIORef' (live_indices db) (Map.delete k)
        ddef<-readIORef $ def db
        let new_indices=filter (/=(I.IndexDef tname target)) $ indices ddef
        
        removeFile $ indexPath (BC.unpack $ name ddef) (I.IndexDef tname target)
        writeIORef (def db) (ddef {indices=new_indices})

    dropAllIndices :: ByteString->Database->IO ()
    dropAllIndices tblname db=do
        ddef<-readIORef $ def db
        let indices_to_remove=filter (\(I.IndexDef t _)->t==tblname) $ indices ddef
        mapM_ (\(I.IndexDef a b)->dropIndex' (a,b) db) indices_to_remove
    serialize :: DatabaseDef->Put
    serialize def=do
        DT.writeString $ name def
        putWord32le $ fromIntegral $ length $ tables def
        mapM_ T.serializeTableDef $ tables def
        putWord32le $ fromIntegral $ length $ indices def
        mapM_ I.serializeIndexDef $ indices def

    deserialize :: Get DatabaseDef
    deserialize=do
        name<-DT.readString
        tables<-DT.getList T.deserializeTableDef
        indices<-DT.getList I.deserializeIndexDef
        return $ DatabaseDef name tables indices

    listDatabases :: IO [String]
    listDatabases=listDirectory "slowql-data"
    
    open :: String->IO (Either String Database)
    open name=do
        e<-doesDirectoryExist ("slowql-data/"++name)
        if e
            then do
                putStrLn "Opening database"
                str<-BS.readFile $ "slowql-data/"++name++"/.slowql"
                let df=runGet deserialize (BL.fromStrict str)
                lt<-openAllTables df
                r1<-newIORef df
                r2<-newIORef lt
                r3<-newIORef (Map.empty)
                let db=Database r1 r2 r3
                openAllIndices db
                return $ Right db
            else return $ Left "Database does not exist."
    close :: Database->IO ()
    close db=do
        lt<-readIORef $ live_tables db
        li<-readIORef $ live_indices db
        mapM_ T.close $ Map.elems lt
        mapM_ I.close $ Map.elems li
        d<-readIORef $ def db
        putStrLn "Closing database"
        BL.writeFile ("slowql-data/"++(BC.unpack $ name d)++"/.slowql") $runPut $ serialize d
    create :: String->IO (Maybe String)
    create db=do
        e<-doesDirectoryExist ("slowql-data/"++db)
        if e then return $ Just "Database already exists."
        else do
                createDirectory $ "slowql-data/"++db
                let def=DatabaseDef (BC.pack db) [] []
                BL.writeFile ("slowql-data/"++db++"/.slowql") $runPut $ serialize def
                return Nothing
    ensureUnique :: String->[a]->a
    ensureUnique err (x:[])=x
    ensureUnique err _=error err

    ensureUniqueMaybe :: [a]->Maybe a
    ensureUniqueMaybe (x:[])=Just x
    ensureUniqueMaybe _=Nothing
    selectFrom :: P.SQLStatement->Database->IO ()
    selectFrom stmt db=do
        (expr, related_tables)<-compileSelect stmt db
        runConduit ((R.evaluate expr related_tables ).|printC)

    drop :: String->IO(Maybe String)
    drop db=do
        e<-doesDirectoryExist ("slowql-data/"++db)
        if e
            then do
                removeDirectoryRecursive $ "slowql-data/"++db
                return Nothing
            else return $ Just "Database does not exist."


    dropTable :: String->Database->IO ()
    dropTable tblname db=do
        [tbl]<-getTablesByName [tblname] db
        T.close tbl
        ddef<-readIORef $ def db
        let new_tables=filter (\d->(T.name d) /= (BC.pack tblname)) (tables ddef)
        writeIORef (def db) ddef{tables=new_tables}
        modifyIORef' (live_tables db) (Map.delete $ BC.pack tblname)
        removeFile $ tablePath (BC.unpack $ name ddef) (tblname)
        dropAllIndices (BC.pack tblname) db
        putStrLn "DROP TABLE"
    getTablesByName :: [String]->Database->IO [T.Table]
    getTablesByName l db=do
        mp<-readIORef $ live_tables db
        return $ map (((Map.!) mp). BC.pack) l
    translateColumn :: [T.TableDef]->P.Column->Maybe (Int, Int, Int, DT.TParam) --TableIndex, ColumnIndex, TotalIndex, TParam
    translateColumn tdefs=
        let domains=map (DT.domains . T.domains) tdefs 
            offsets=DT.buildArray' $ scanl (+) 0 (map (\arr->let (a,b)=Arr.bounds arr in b+1) domains)
            all_columns=map (\tabledef->let column_names=map (\p->(DT.name $ DT.general p, p)) $ Arr.elems tabledef in zip [0..] column_names) domains
            concat_columns=concat $ zipWith (\tindex list->map (\(cindex, (name,p))->(name, (tindex, cindex, (offsets!tindex)+cindex, p))) list)[0..] all_columns
            go (P.ForeignCol tblname colname)=do
                (tindex, tabledef)<-find (\(index, t)->BC.pack tblname==(T.name t)) (zip [0..] tdefs)
                let table_domains=DT.domains $ T.domains tabledef
                (cindex, p)<-find (\(_, d)->(BC.pack colname)==(DT.name $ DT.general d)) (zip [0..] $ Arr.elems table_domains)
                return (tindex, cindex, (offsets!tindex)+cindex, p)
            go (P.LocalCol colname)=do
                (_, t)<-ensureUniqueMaybe $ filter (\(bs, tuple)->BC.pack colname==bs) concat_columns
                return t
        in go
    forceJust :: String->Maybe a->a
    forceJust err Nothing=error err
    forceJust _ (Just x)=x
    applyOp :: P.Op->DT.TValue->DT.TValue->Bool
    applyOp P.OpEq=(==)
    applyOp P.OpNeq=(/=)
    applyOp P.OpLeq=(<=)
    applyOp P.OpGeq=(>=)
    applyOp P.OpLt=(<)
    applyOp P.OpGt=(>)

    compileSelect :: P.SQLStatement->Database->IO (R.RelExpr, Arr.Array Int T.Table)
    compileSelect stmt db=do
        df<-readIORef $ def db
        let get_tables (P.SelectStmt _ a b c)=(a,b,c)
            get_tables (P.SelectAllStmt a b c)=(a,b,c)
        let (used_tables, whereclause, suffices)=get_tables stmt
        used_tables_list<-getTablesByName used_tables db -- param 2
        let all_table_defs=map T.def used_tables_list
        let translate=(forceJust "Column missing!" ). (translateColumn all_table_defs)
        --Expand where-clause into where segments
        let expand_where P.WhereAny=[]
            expand_where (P.WhereOp column op (P.ExprV val))=let (t,c,_, p)=translate column in if DT.compatiblePV p val then [R.SingleTableWhereClause t (\r->applyOp op ((DT.fields r) ! c) val)] else [R.WhereNotCompatiblePV t c]
            expand_where (P.WhereOp c1 op (P.ExprC c2))=
                let (t1, ci1, i1, p1)=translate c1
                    (t2, ci2, i2, p2)=translate c2
                in if DT.compatiblePP p1 p2
                    then if t1==t2
                        then [R.SingleTableWhereClause t1 (\r->applyOp op ((DT.fields r) ! ci1) ((DT.fields r) ! ci2))]
                        else [R.GeneralWhereClause (\r->applyOp op ((DT.fields r) ! i1) ((DT.fields r) ! i2))]
                    else [R.WhereNotCompatiblePP t1 ci1 t2 ci2]
            expand_where (P.WhereIsNull column isnull)=let (t, c, _, _)=translate column in [R.SingleTableWhereClause t (\r->(DT.isNull ((DT.fields r)!c))==isnull)]
            expand_where (P.WhereAnd a b)=(expand_where a)++(expand_where b)
        
        let compiled_whereclause=force $ expand_where whereclause
        mapM_ R.assertCompileError compiled_whereclause
        --let table_sources=DT.buildArray' map R.RelEnumAll [0..((length used_tables_list)-1)]
        table_vec<-V.new (length used_tables_list) :: IO (V.IOVector R.RelExpr)
        mapM_ (uncurry $ V.write table_vec ) $ zip [0..] $ map R.RelEnumAll [0..((length used_tables_list)-1)]

        --Maybe need reorder?
        --Apply single-table filter
        let apply_singletable (R.SingleTableWhereClause tid pred)=V.modify table_vec (R.RelFilter $ R.RecordPred pred) tid :: IO()
            apply_singletable _=return ()
        mapM_ apply_singletable compiled_whereclause
        cart_items<-mapM (V.read table_vec) [0..((length used_tables_list)-1)]
        let folded_expr=foldr1 R.RelCart cart_items --Not considering index.
        let apply_multitable expr (R.GeneralWhereClause pred)  =R.RelFilter (R.RecordPred pred) expr
            apply_multitable expr _=expr
        let full_expr=foldl' apply_multitable folded_expr compiled_whereclause
        --Expand projectors
        let names_of_all_columns=concat $ map (map (DT.name . DT.general) . Arr.elems . DT.domains . T.domains . T.def) used_tables_list
        let (removed_rids, _)=unzip $ filter (\(_, n)->n==BC.pack "_rid") $ zip [0..] names_of_all_columns 
        let add_projector (P.SelectAllStmt _ _ _) expr=R.RelProjection removed_rids expr
            add_projector (P.SelectStmt proj _ _ _) expr=R.RelProjection (map (\((_, _, c, _))->c) $ map translate proj) expr
        
        -- Add offset
        let apply_offset expr (P.COffset offset)=R.RelSkip offset expr
            apply_offset expr _=expr
        -- Add limit
        let apply_limit expr (P.CLimit limit)=R.RelTake limit expr
            apply_limit expr _=expr
        let query_plan=(flip (foldl' apply_limit) suffices) $ (flip (foldl' apply_offset) suffices)$ add_projector stmt full_expr
        evaluate $ force query_plan
        putStrLn ("Query plan: "++(show query_plan))
        return (query_plan, DT.buildArray' used_tables_list)
        
    doInsert :: String->[[DT.TValue]]->Database->IO ()
    doInsert tblname orig_tvalues db=do
        [tbl]<-getTablesByName [tblname] db
        --do pre-flight check
        uuids<-mapM (const generateUUID) orig_tvalues
        let tvalues=zipWith (:) (map (DT.ValChar . Just ) uuids) orig_tvalues
        mapM_ (\recd->T.insert tbl (DT.Record $ DT.buildArray' recd)) tvalues
        putStrLn ("INSERT "++(show $ length tvalues))
    doUpdate :: String->[(String, DT.TValue)]->P.WhereClause->Database->IO()
    doUpdate tblname tvalues wc db=do
        [tbl]<-getTablesByName [tblname] db
        let translate=(forceJust "Column missing!") .(translateColumn [T.def tbl] ) 
        let (column_names, values)=unzip tvalues
        let columns= map (translate . P.LocalCol) column_names
        --Check compatible
        let check_compatible=and $ zipWith (\v (_,_,_,p)->DT.compatiblePV p v) values columns
        assertIO check_compatible "Some column and value not compatible!"
        let expand_where P.WhereAny=[]
            expand_where (P.WhereOp column op (P.ExprV val))=let (t,c,_, p)=translate column in if DT.compatiblePV p val then [R.SingleTableWhereClause t (\r->applyOp op ((DT.fields r) ! c) val)] else [R.WhereNotCompatiblePV t c]
            expand_where (P.WhereOp c1 op (P.ExprC c2))=
                let (t1, ci1, i1, p1)=translate c1
                    (t2, ci2, i2, p2)=translate c2
                in if DT.compatiblePP p1 p2
                    then if t1==t2
                        then [R.SingleTableWhereClause t1 (\r->applyOp op ((DT.fields r) ! ci1) ((DT.fields r) ! ci2))]
                        else [R.GeneralWhereClause (\r->applyOp op ((DT.fields r) ! i1) ((DT.fields r) ! i2))]
                    else [R.WhereNotCompatiblePP t1 ci1 t2 ci2]
            expand_where (P.WhereIsNull column isnull)=let (t, c, _, _)=translate column in [R.SingleTableWhereClause t (\r->(DT.isNull ((DT.fields r)!c))==isnull)]
            expand_where (P.WhereAnd a b)=(expand_where a)++(expand_where b)
        let compiled_where=expand_where wc
        mapM_ R.assertCompileError compiled_where
        let eval_where rc (R.SingleTableWhereClause _ pred)=pred rc
        --Do the real update work
        let replacement_tree=zipWith (\v (_, i, _, _)->(i, v)) values columns
        let checker rc=let pass w=eval_where rc w in foldr (&&) True (map pass compiled_where)
        let updator rc=if checker rc
            then Just $ DT.Record $ Arr.accum (\o n->n) (DT.fields rc) replacement_tree
            else Nothing
        updated<-T.update tbl updator
        putStrLn ("UPDATE "++(show updated))
    doDelete :: String->P.WhereClause->Database->IO ()
    doDelete tblname wc db=do
        [tbl]<-getTablesByName [tblname] db
        let translate=(forceJust "Column missing!") .(translateColumn [T.def tbl] ) 
        let expand_where P.WhereAny=[]
            expand_where (P.WhereOp column op (P.ExprV val))=let (t,c,_, p)=translate column in if DT.compatiblePV p val then [R.SingleTableWhereClause t (\r->applyOp op ((DT.fields r) ! c) val)] else [R.WhereNotCompatiblePV t c]
            expand_where (P.WhereOp c1 op (P.ExprC c2))=
                let (t1, ci1, i1, p1)=translate c1
                    (t2, ci2, i2, p2)=translate c2
                in if DT.compatiblePP p1 p2
                    then if t1==t2
                        then [R.SingleTableWhereClause t1 (\r->applyOp op ((DT.fields r) ! ci1) ((DT.fields r) ! ci2))]
                        else [R.GeneralWhereClause (\r->applyOp op ((DT.fields r) ! i1) ((DT.fields r) ! i2))]
                    else [R.WhereNotCompatiblePP t1 ci1 t2 ci2]
            expand_where (P.WhereIsNull column isnull)=let (t, c, _, _)=translate column in [R.SingleTableWhereClause t (\r->(DT.isNull ((DT.fields r)!c))==isnull)]
            expand_where (P.WhereAnd a b)=(expand_where a)++(expand_where b)
        let compiled_where=expand_where wc
        mapM_ R.assertCompileError compiled_where
        let eval_where rc (R.SingleTableWhereClause _ pred)=pred rc
        -- Do the real delete work
        let deleter rc=let pass w=eval_where rc w in foldr (&&) True (map pass compiled_where)
        deleted<-T.delete tbl deleter
        putStrLn ("DELETE "++(show deleted))

    describeTable :: String->Database->IO()
    describeTable tblname db=do
        [tbl]<-getTablesByName [tblname] db
        print $ T.def tbl
    --compileUpdate stmt db=return ()
    --compileDelete stmt db=return ()

    --data Database=Database {file :: Han}
    --createDatabase :: String->IO ()
    --createDatabase=do
