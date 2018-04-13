module Mongo.MQLv1.MatchExpr(
    MatchExpr(..),

    evalMatchExpr,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value

data MatchExpr
    -- Comparisons.
    = EqMatchExpr Path Value
    | LTEMatchExpr Path Value
    | LTMatchExpr Path Value
    | GTMatchExpr Path Value
    | GTEMatchExpr Path Value
    | NEMatchExpr Path Value

    -- This is $exists:true.
    --
    -- TODO: For now we assume that $exists:false is parsed to NotExpr (ExistsMatchExpr ..). In the
    -- future, we may wish to express $exists:false in terms of a flavor of match expression where
    -- all selected elements must match (and the document vacuously matches if there are no such
    -- elements). For the moment, however, match expressions always have "any selected element
    -- matches" semantics.
    | ExistsMatchExpr Path

    -- Logical match expressions. The parser is responsible for converting $nor into $not -> $or.
    | AndMatchExpr [MatchExpr]
    | OrMatchExpr [MatchExpr]
    | NotMatchExpr MatchExpr

-- Given a flag indicating array traversal behavior, the name of a function to apply, and the name
-- of a bound variable containing a value:
-- * If there is no implicit array traversal, returns a core expr which applies the named function
--   to the value bound to the var.
-- * If there is implicit array traversal, returns a core expr which folds over the array.
makeApplyFuncExpr :: ImplicitArrayTraversal -> String -> String -> CoreExpr Value
makeApplyFuncExpr NoImplicitTraversal funcName retrievedVal = FunctionApp funcName [Var retrievedVal]

makeApplyFuncExpr ImplicitlyTraverseArrays funcName retrievedVal =
    If (IsArray (Var retrievedVal))
        (let foldFuncName = "fold_" ++ funcName in
            FunctionDef foldFuncName (Function ["v", "init"]
                (PutBool (Or (GetBool (Var "init")) (GetBool (FunctionApp funcName [Var "v"])))))
                (PutBool (FoldBool
                    foldFuncName
                    (GetBool (Const (BoolValue False)))
                    (Var retrievedVal))))
        (FunctionApp funcName [Var retrievedVal])

-- Generates a core expression which traverses the given path, and applies the named function to
-- each leaf element. The expression returns true if the named function ever returns true.
makeTraversePathExpr :: Path -> String -> CoreExpr Value
makeTraversePathExpr [] funcName = FunctionApp funcName [Var "ROOT"]

makeTraversePathExpr (PathComponent (FieldName f) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ f ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "value_of_" ++ f in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsDocument (Var formalParam)) (HasField f (GetDocument (Var formalParam))))
                (Let retrievedVal (SelectField f (GetDocument (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (ArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_" ++ show i in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem i (GetArray (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (FieldNameOrArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "arr_index_or_field_" ++ show i in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem i (GetArray (Var formalParam)))
                    -- XXX: In MQLv1, there is no implicit array traversal when a path component
                    -- that can act as either an array index or a field name resolves to an array
                    -- index. That is, the document {a: {0: [1, 2, 3]}} matches the query
                    -- {"a.0": 3}, but the document {a: [[1, 2, 3]]} does not match.
                    (FunctionApp funcName [Var retrievedVal]))
                (If (And (IsDocument (Var formalParam))
                        (HasField (show i) (GetDocument (Var formalParam))))
                    (Let retrievedVal (SelectField (show i) (GetDocument (Var formalParam)))
                        (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                    (Const $ BoolValue False))))
            (makeTraversePathExpr rest nextLevelFuncName)

-- Helper for desugaring $lt, $lte, $gt, $gte, and $eq. The passed Function is the leaf predicate
-- that should be applied to each of the elements selected by the Path. The Value is the value which
-- the match expr is comparing against.
--
-- Returns the CoreExpr resulting from desugaring.
desugarComparisonMatchExpr :: Path -> Value -> Function -> CoreExpr Bool

-- XXX: Comparing to an array in MQLv1 matches if either any of the elements in the array matches,
-- or if the array as a whole matches. For example, {a: {$eq: [1, 2, 3]}} matches both the documents
-- {a: [1, 2, 3]} and {a: [0, [1, 2, 3]]}. The path selection behavior is therefore implicitly
-- encoded in the fact that this is an array comparison. Instead, users should probably have to say
-- explicitly "find me documents where 'a' is an array containing an element [1, 2, 3]" or "find me
-- documents where field 'a' contains exactly the array [1, 2, 3]".
desugarComparisonMatchExpr path (ArrayValue _) pred =
    GetBool $ FunctionDef "pred" pred (PutBool
        (Or (GetBool (makeTraversePathExpr (reverse path) "pred"))
            (GetBool (makeTraversePathExpr
                (reverse $ convertPathToNotTraverseTrailingArrays path) "pred"))))

desugarComparisonMatchExpr path val pred =
    GetBool $ FunctionDef "pred" pred (makeTraversePathExpr (reverse path) "pred")

-- Compiles a MatchExpr into a core language expression.
desugarMatchExpr :: MatchExpr -> CoreExpr Bool
desugarMatchExpr (EqMatchExpr path val) =
    desugarComparisonMatchExpr
        path val (Function ["x"] (PutBool $ CompareEQ (Var "x") (Const val)))

desugarMatchExpr (LTMatchExpr path val) =
    desugarComparisonMatchExpr
        path val (Function ["x"] (PutBool $ CompareLT (Var "x") (Const val)))

desugarMatchExpr (LTEMatchExpr path val) =
    desugarComparisonMatchExpr
        path val (Function ["x"] (PutBool $ CompareLTE (Var "x") (Const val)))

desugarMatchExpr (GTMatchExpr path val) =
    desugarComparisonMatchExpr
        path val (Function ["x"] (PutBool $ CompareGT (Var "x") (Const val)))

desugarMatchExpr (GTEMatchExpr path val) =
    desugarComparisonMatchExpr
        path val (Function ["x"] (PutBool $ CompareGTE (Var "x") (Const val)))

desugarMatchExpr (NEMatchExpr path val) =
    Not $ desugarMatchExpr (EqMatchExpr path val)

desugarMatchExpr (ExistsMatchExpr path) =
    GetBool $ FunctionDef "exists" (Function ["x"] (Const $ BoolValue True))
        (makeTraversePathExpr (reverse $ convertPathToNotTraverseTrailingArrays path) "exists")

desugarMatchExpr (NotMatchExpr expr) = Not $ desugarMatchExpr expr

desugarMatchExpr (AndMatchExpr []) = GetBool $ Const (BoolValue True)
desugarMatchExpr (AndMatchExpr [hd]) = desugarMatchExpr hd
desugarMatchExpr (AndMatchExpr (hd:tail)) =
    And (desugarMatchExpr hd) (desugarMatchExpr (AndMatchExpr tail))

desugarMatchExpr (OrMatchExpr []) = GetBool $ Const (BoolValue False)
desugarMatchExpr (OrMatchExpr [hd]) = desugarMatchExpr hd
desugarMatchExpr (OrMatchExpr (hd:tail)) =
    Or (desugarMatchExpr hd) (desugarMatchExpr (OrMatchExpr tail))

-- Returns true if the Value matches the MatchExpr. Otherwise returns false. Any Error return value
-- is query-fatal.
evalMatchExpr :: MatchExpr -> Value -> Either Error Bool
evalMatchExpr matchExpr value =
    evalCoreExpr (desugarMatchExpr matchExpr)
        Environment { boundVariables = [("ROOT", value)], definedFunctions = [] }
