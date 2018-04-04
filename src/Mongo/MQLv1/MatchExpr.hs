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
    = EqMatchExpr Path Value
    | LTEMatchExpr Path Value
    | LTMatchExpr Path Value
    | GTMatchExpr Path Value
    | GTEMatchExpr Path Value

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

-- Compiles a MatchExpr into a core language expression.
desugarMatchExpr :: MatchExpr -> CoreExpr Bool
desugarMatchExpr (EqMatchExpr path val) =
    GetBool $ FunctionDef "eq" (Function ["x"] (PutBool $ CompareEQ (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "eq")

desugarMatchExpr (LTMatchExpr path val) =
    GetBool $ FunctionDef "lt" (Function ["x"] (PutBool $ CompareLT (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "lt")

desugarMatchExpr (LTEMatchExpr path val) =
    GetBool $ FunctionDef "lte" (Function ["x"] (PutBool $ CompareLTE (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "lte")

desugarMatchExpr (GTMatchExpr path val) =
    GetBool $ FunctionDef "gt" (Function ["x"] (PutBool $ CompareGT (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "gt")

desugarMatchExpr (GTEMatchExpr path val) =
    GetBool $ FunctionDef "gte" (Function ["x"] (PutBool $ CompareGTE (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "gte")

-- Returns true if the Value matches the MatchExpr. Otherwise returns false. Any Error return value
-- is query-fatal.
evalMatchExpr :: MatchExpr -> Value -> Either Error Bool
evalMatchExpr matchExpr value =
    evalCoreExpr (desugarMatchExpr matchExpr)
        Environment { boundVariables = [("ROOT", value)], definedFunctions = [] }
