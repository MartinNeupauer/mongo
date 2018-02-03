import Text.JSON
import Text.JSON.Pretty
import Mongo.Bool3VL
import Mongo.EvalExpr
import Mongo.Expression
import Mongo.Util
import Mongo.Variant
import Data.Monoid
import Data.List

type Array2 = [Variant2]

data Variant2 =
    MkNull |
    MkInt Int |
    MkStr String |
    MkVarDoc Document2 |
    MkVarArray Array2
    deriving (Eq, Show)

data Document2 =
    MkDoc [(String, Variant2)]
    deriving (Eq, Show)

toJSONM::Maybe Variant2->String
toJSONM (Just x) = toJSON x
toJSONM Nothing = "nothing"

toJSON::Variant2->String
toJSON v =
    case v of
        MkNull -> "null"
        (MkInt i) -> show i
        (MkStr s) -> "\"" ++ s ++ "\""
        (MkVarDoc d) -> toJSONDoc d
        (MkVarArray a) -> toJSONArray a

toJSONDoc::Document2->String  
toJSONDoc (MkDoc l) =
    "{" ++ (toJSONDocFromList l "") ++ "}"

toJSONDocFromList::[(String, Variant2)]->String->String
toJSONDocFromList [x] accum = (accum ++ "\"" ++ (fst x)++ "\": " ++ toJSON (snd x))
toJSONDocFromList (x:xs) accum = toJSONDocFromList xs (accum ++ "\"" ++ (fst x)++ "\": " ++ toJSON (snd x) ++ ", ")
toJSONDocFromList [] accum = accum

toJSONArray::Array2->String
toJSONArray a =
    "[" ++ (toJSONArrayFromList a "") ++ "]"

toJSONArrayFromList::Array2->String->String
toJSONArrayFromList [x] accum = accum ++ (toJSON x)
toJSONArrayFromList (x:xs) accum = toJSONArrayFromList xs (accum ++ (toJSON x) ++ ", ")
toJSONArrayFromList [] accum = accum

type FilterFn = Variant2 -> Bool3VL

getFieldFromList::[(String, Variant2)]->String->Maybe Variant2
getFieldFromList (x:xs) fieldName =
    if ((fst x) == fieldName)
    then
        Just (snd x)
    else
        getFieldFromList xs fieldName

getFieldFromList [] _ = Nothing

getField2::Document2->String->Maybe Variant2
getField2 (MkDoc l) fieldName = getFieldFromList l fieldName

getElement2::Array2->Int->Maybe Variant2
getElement2 a idx =
    if (idx >=0 && idx < length a)
    then
        Just (a !! idx)
    else
        Nothing


data PathExpr =
    SelectIdentity |
    SelectField2 String |
    SelectElement2 Int |
    SelectFilter FilterFn |
    SelectApply PathExpr PathExpr |
    SelectRecur PathExpr

select::Variant2->PathExpr->Maybe Variant2
select v path =
    case path of
        (SelectIdentity) ->
            Just v
        (SelectField2 f) ->
            case v of
                (MkVarDoc d) -> getField2 d f
                otherwise -> Nothing
        (SelectElement2 idx) ->
            case v of
                (MkVarArray a) -> getElement2 a idx
                otherwise -> Nothing
        (SelectFilter f) ->
            if (f v == True3VL)
            then
                Just v
            else
                Nothing
        (SelectApply lhs rhs) ->
            let leftv = select v lhs in
                case leftv of
                    Just x -> select x rhs
                    Nothing -> Nothing
        (SelectRecur innerPath) ->
            selectRec v innerPath

selectRec::Variant2->PathExpr->Maybe Variant2
selectRec v path =
    let toplevel = select v path
    in
        let accum = case toplevel of
                        Just x -> [x]
                        Nothing -> []
        in
            case v of
                (MkVarDoc d) ->
                    let result = selectFromDocument d path accum
                    in
                        case result of
                            (x:xs) -> Just (MkVarArray (x:xs))
                            [] -> Nothing          
                (MkVarArray a) ->
                    let result = selectFromArray a path accum
                    in
                        case result of
                            (x:xs) -> Just (MkVarArray (x:xs))
                            [] -> Nothing          
                otherwise ->
                    case accum of
                        (x:xs) -> Just (MkVarArray (x:xs))
                        [] -> Nothing          


selectFromArray::Array2->PathExpr->[Variant2]->[Variant2]
selectFromArray (x:xs) path accum =
    let sel = selectRec x path
    in
        let newaccum = case sel of
                            Just (MkVarArray (x:xs)) -> (x:xs ++ accum)
                            Nothing -> accum
        in
            selectFromArray xs path newaccum

selectFromArray [] _ accum =
    accum

selectFromList::[(String,Variant2)]->PathExpr->[Variant2]->[Variant2]
selectFromList (x:xs) path accum =
    let sel = selectRec (snd x) path
    in
        let newaccum = case sel of
                            Just (MkVarArray (x:xs)) -> (x:xs ++ accum)
                            Nothing -> accum
        in
            selectFromList xs path newaccum

selectFromList [] _ accum =
    accum

selectFromDocument (MkDoc l) path accum = selectFromList l path accum

d1 = MkDoc []
d2 = MkDoc [("a", MkInt 5), ("b", MkStr "bla"), ("c", MkVarDoc d1)]
v2 = MkVarDoc d2
d3 = MkDoc [("a", MkInt 10), ("b", v2)]
v3 = MkVarDoc d3
d4 = MkDoc [("q", MkInt 20), ("r", MkVarArray [v3,v3,v2])]
v4 = MkVarDoc d4
d5 = MkDoc [("a", MkVarArray[])]
v5 = MkVarDoc d5

s2 = select v2 (SelectField2 "c")
s3 = select v3 (SelectField2 "c")
s31 = select v3 (SelectRecur (SelectField2 "aa"))
{-
dafunc::Int->Expr a
dafunc a =
    if a == 0
    then
        GetInt (Const (IntValue 5))
    else
        GetInt (Const (IntValue 5))
-}

dafunc::Expr a -> Int -> Expr a
dafunc x y = x

testtest = 
    do
        f <- readFile "test1.json"
        let decoded = decode f :: Result (JSObject JSValue)

        case decoded of
            Ok val -> putStrLn $ render (pp_js_object val)
            Error e -> putStrLn e

