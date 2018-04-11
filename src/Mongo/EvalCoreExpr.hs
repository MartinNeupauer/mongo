{-# LANGUAGE GADTs, LambdaCase #-}

module Mongo.EvalCoreExpr(
    Environment(..),

    evalCoreExpr,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.Value
import qualified Data.Maybe

-- An environment maps a variable name to an associated bound value. It separately stores the list
-- of defined functions.
data Environment = Environment {
    boundVariables :: [(String, Value)],
    definedFunctions :: [(String, Function)]
    }

-- Adds a (variable name => Value) mapping to the given environment. It is _not_ an error if a
-- binding for the variable already exists in the input Enviornment. In this case, the old binding
-- is replaced with a binding to the new value in the returned Environment.
bindVar :: Environment -> String -> Value -> Environment
bindVar env var bindVal =
    let newVarBindings = (var, bindVal) : filter (\(x, _) -> x /= var) (boundVariables env) in
        Environment { boundVariables = newVarBindings, definedFunctions = definedFunctions env }

-- Get a function out of the given environment, or error if no such function exists.
lookupFunction :: String -> Environment -> Either Error Function
lookupFunction name env =
    case lookup name (definedFunctions env) of
        Just f -> Right f
        _ -> Left Error { errCode = UnknownFunction, errReason = "Unknown function: " ++ name }

applyFunction :: Function -> [CoreExpr Value] -> Environment -> Either Error Value
applyFunction (Function argNames body) argList env =
    if length argNames /= length argList
    then
        Left Error {
            errCode = WrongNumberOfFunctionArgs,
            errReason = "Function taking " ++ show (length argNames) ++
                        " arguments applied with " ++ show (length argList) }
    else do
        -- Evaluate all of the arguments eagerly.
        resultVals <- mapM (`evalCoreExpr` env) argList
        -- New environment consists only of the globally defined function definitions and
        -- bindings for each of the formal arguments. Be careful not to propagate variable bindings
        -- which are currently in the environment, otherwise we end up with dynamic scoping!
        let newEnvInit = Environment {
                boundVariables = [],
                definedFunctions = definedFunctions env }
            newEnv = foldr (\x y -> uncurry (bindVar y) x) newEnvInit $ zip argNames resultVals in
                evalCoreExpr body newEnv

evalFoldBool :: Function -> Bool -> Value -> Environment -> Either Error Bool
evalFoldBool _ initVal (ArrayValue Array { getElements = [] }) _ = Right initVal
evalFoldBool func initVal (ArrayValue Array { getElements = hd:rest }) env =
    applyFunction func [Const hd, Const (BoolValue initVal)] env >>=
        (\case (BoolValue newInit) -> evalFoldBool func newInit
                                        (ArrayValue Array { getElements = rest }) env)

evalFoldBool _ initVal (DocumentValue Document { getFields = [] }) _ = Right initVal
evalFoldBool func initVal (DocumentValue Document { getFields = (_, firstValue) : rest } ) env =
    applyFunction func [Const firstValue, Const (BoolValue initVal)] env >>=
        (\case (BoolValue newInit) -> evalFoldBool func newInit
                                        (DocumentValue Document { getFields = rest }) env)

-- Folding over scalars is allowed; this means that we simply apply the given function to the
-- scalar.
evalFoldBool func initVal scalarValue env =
    applyFunction func [Const scalarValue, Const (BoolValue initVal)] env
        >>= (\case (BoolValue result) -> Right result)

evalCoreExpr :: CoreExpr a -> Environment -> Either Error a
evalCoreExpr (Const c) _ = return c

-- Selectors
evalCoreExpr (SelectField f v) env =
    evalCoreExpr v env >>= getField f

evalCoreExpr (SelectElem i v) env =
    evalCoreExpr v env >>= getElement i

evalCoreExpr (SetField (f,v) d) env =
    do
        doc <- evalCoreExpr d env
        val <- evalCoreExpr v env
        return $ addField (f,val) (removeField f doc)

evalCoreExpr (RemoveField f v) env =
    removeField f <$> evalCoreExpr v env

evalCoreExpr (HasField f v) env =
    hasField f <$> evalCoreExpr v env

evalCoreExpr (ArrayLength arr) env =
    arrayLength <$> evalCoreExpr arr env

evalCoreExpr (GetInt v) env =
    evalCoreExpr v env >>= getIntValue

evalCoreExpr (GetBool v) env =
    evalCoreExpr v env >>= getBoolValue

evalCoreExpr (GetString v) env =
    evalCoreExpr v env >>= getStringValue

evalCoreExpr (GetArray v) env =
    evalCoreExpr v env >>= getArrayValue

evalCoreExpr (GetDocument v) env =
    evalCoreExpr v env >>= getDocumentValue

evalCoreExpr (PutInt v) env =
    IntValue <$> evalCoreExpr v env

evalCoreExpr (PutBool b) env =
    BoolValue <$> evalCoreExpr b env

evalCoreExpr (PutDocument v) env =
    DocumentValue <$> evalCoreExpr v env

--
-- Type predicates.
--

evalCoreExpr (IsNull v) env =
    isNull <$> evalCoreExpr v env

evalCoreExpr (IsUndefined expr) env =
    isUndefined <$> evalCoreExpr expr env

evalCoreExpr (IsInt expr) env =
    isInt <$> evalCoreExpr expr env

evalCoreExpr (IsBool expr) env =
    isBool <$> evalCoreExpr expr env

evalCoreExpr (IsString expr) env =
    isString <$> evalCoreExpr expr env

evalCoreExpr (IsArray a) env =
    isArray <$> evalCoreExpr a env

evalCoreExpr (IsDocument d) env =
    isDocument <$> evalCoreExpr d env

-- Arithmetic
evalCoreExpr (Plus lhs rhs) env =
    (+) <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (Minus lhs rhs) env =
    (-) <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

-- Logical.
evalCoreExpr (And lhs rhs) env =
    do
        lhsVal <- evalCoreExpr lhs env
        if not lhsVal then Right False else evalCoreExpr rhs env

evalCoreExpr (Or lhs rhs) env =
    do
        lhsVal <- evalCoreExpr lhs env
        if lhsVal then Right True else evalCoreExpr rhs env

evalCoreExpr (Not e) env =
    not <$> evalCoreExpr e env

-- Comparisons.
evalCoreExpr (CompareEQ lhs rhs) env =
    compareEQ <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareLT lhs rhs) env =
    compareLT <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareLTE lhs rhs) env =
    compareLTE <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareGT lhs rhs) env =
    compareGT <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareGTE lhs rhs) env =
    compareGTE <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (CompareEQ3VL lhs rhs) env =
    compareEQ3VL <$> evalCoreExpr lhs env <*> evalCoreExpr rhs env

evalCoreExpr (If cond t e) env =
    do
        condval <- evalCoreExpr cond env
        if condval
        then
            evalCoreExpr t env
        else
            evalCoreExpr e env

-- Extract the value of a variable from the environment.
evalCoreExpr (Var var) env = case lookup var (boundVariables env) of
    Just v -> Right v
    _ -> Left Error { errCode = UnboundVariable, errReason = "Unbound variable: " ++ var }

evalCoreExpr (Let var bindExpr inExpr) env =
    do
        -- Determine the value to which 'var' should be bound. Be sure to use the *old* environment.
        bindVal <- evalCoreExpr bindExpr env
        -- Evaluate the 'in' expression using the new environment obtained by adding a binding.
        evalCoreExpr inExpr (bindVar env var bindVal)

evalCoreExpr (FunctionDef name f rest) env =
    -- Functions have expression-global scope, so it is illegal to redefine a function name.
    if Data.Maybe.isJust (lookup name (definedFunctions env))
    then Left Error {
        errCode = FunctionRedefinition,
        errReason = "Function defined multiple times: " ++ name }
    -- Add the function to the environment, replacing any existing function of the same name.
    else
        let newFuncBindings = (name, f) : definedFunctions env
            newEnv = Environment {
            boundVariables = boundVariables env,
            definedFunctions =  newFuncBindings } in
                -- Evaluate the 'in' expression using the *new* environment.
                evalCoreExpr rest newEnv

evalCoreExpr (FunctionApp name argList) env =
    do
        func <- lookupFunction name env
        applyFunction func argList env

evalCoreExpr (FoldBool funcName init toFold) env =
    do
        func <- lookupFunction funcName env
        initVal <- evalCoreExpr init env
        toFoldVal <- evalCoreExpr toFold env
        evalFoldBool func initVal toFoldVal env
