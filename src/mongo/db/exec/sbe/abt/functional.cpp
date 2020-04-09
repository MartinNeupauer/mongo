/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/exe_generator.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
FDep::FDep(Type typeIn, std::vector<ABT> deps) : Base(std::move(deps)), _type(std::move(typeIn)) {
    checkValueSyntaxSort(nodes());
}

EvalPath::EvalPath(ABT pathIn, ABT inputIn)
    : Base(std::move(pathIn), std::move(inputIn)), _type(variantType()) {
    checkPathSyntaxSort(path());
    checkValueSyntaxSort(input());
}

FunctionCall::FunctionCall(std::string nameIn, std::vector<ABT> argsIn)
    : Base(std::move(argsIn)), _type(kNoType), _name(std::move(nameIn)) {
    checkValueSyntaxSort(nodes());
}

If::If(ABT condIn, ABT thenIn, ABT elseIn)
    : Base(std::move(condIn), std::move(thenIn), std::move(elseIn)) {
    checkValueSyntaxSort(get<0>());
    checkValueSyntaxSort(get<1>());
    checkValueSyntaxSort(get<2>());
}

BinaryOp::BinaryOp(Op opIn, ABT lhs, ABT rhs)
    : Base(std::move(lhs), std::move(rhs)), _type(kNoType), _op(opIn) {
    checkValueSyntaxSort(get<0>());
    checkValueSyntaxSort(get<1>());
}

UnaryOp::UnaryOp(Op opIn, ABT arg) : Base(std::move(arg)), _type(kNoType), _op(opIn) {
    checkValueSyntaxSort(get<0>());
}

LocalBind::LocalBind(ABT bindIn, ABT inIn) : Base(std::move(bindIn), std::move(inIn)) {
    uassert(ErrorCodes::InternalError, "binder expected", get<0>().is<ValueBinder>());
    checkValueSyntaxSort(get<1>());
}

LambdaAbstraction::LambdaAbstraction(ABT paramIn, ABT bodyIn)
    : Base(std::move(paramIn), std::move(bodyIn)), _type(kNoType) {
    uassert(ErrorCodes::InternalError, "binder expected", get<0>().is<ValueBinder>());
    checkValueSyntaxSort(get<1>());
}

OptFence::OptFence(ABT input) : Base(std::move(input)) {
    checkValueSyntaxSort(get<0>());
}

const ABT& OptFence::followFence() const {
    return follow(get<0>());
}
ABT& OptFence::followFence() {
    return follow(get<0>());
}
/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, LocalBind& op, ABT* bind, ABT* in) {
    mergeVarsHelper(&e, bind);
    // pull out free variables from the in expression
    mergeFreeVars(&e, in);
    // resolve free variables against current set of defined variables
    resolveVars(&e, &e);
    // drop locally bound variables as they cannot escape (good old lexical scoping)
    resetDefinedVars(&e);

    // this should be always empty?
    mergeDefinedVars(&e, in);

    return &e;
}

ABT* FreeVariables::transport(ABT& e, LambdaAbstraction& op, ABT* param, ABT* body) {
    mergeVarsHelper(&e, param);
    // pull out free variables from the in expression
    mergeFreeVars(&e, body);
    // resolve free variables against current set of defined variables
    resolveVars(&e, &e);
    // drop locally bound variables as they cannot escape (good old lexical scoping)
    resetDefinedVars(&e);

    // this should be always empty?
    mergeDefinedVars(&e, body);

    return &e;
}
/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walk(const FDep& op, const std::vector<ABT>& deps) {
    // dont generate anything for deps
    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const EvalPath& op, const ABT& path, const ABT& input) {

    auto inputRes = generate(input);
    invariant(inputRes.expr);
    value::SlotId inputSlot;
    if (inputRes.slot) {
        inputSlot = *inputRes.slot;
    } else {
        inputSlot = _slotIdGen.generate();
        _currentStage =
            makeProjectStage(std::move(_currentStage), inputSlot, std::move(inputRes.expr));
    }

    auto saveCtx = _pathCtx;
    PathContext localPathCtx;
    _pathCtx = &localPathCtx;
    _pathCtx->topLevelTraverse = true;
    _pathCtx->expr = makeE<EVariable>(inputSlot);
    _pathCtx->slot = inputSlot;
    _pathCtx->inputMkObjSlot = inputSlot;

    if (path.is<PathGet>()) {
        // return a value
        auto pathRes = generate(path);
    } else {
        // return an object/document
        auto pathRes = generate(path);
        generatePathMkObj();
    }

    GenResult result;
    result.expr = std::move(_pathCtx->expr);
    _pathCtx = saveCtx;
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const FunctionCall& op, const std::vector<ABT>& args) {
    std::vector<std::unique_ptr<EExpression>> argExprs;
    for (const auto& a : args) {
        auto r = generate(a);
        invariant(r.expr);
        argExprs.emplace_back(std::move(r.expr));
    }
    GenResult result;
    result.expr = makeE<EFunction>(op.name(), std::move(argExprs));
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const If& op,
                                           const ABT& cond,
                                           const ABT& thenBranch,
                                           const ABT& elseBranch) {
    auto condResult = generate(cond);
    invariant(condResult.expr);
    auto thenResult = generate(thenBranch);
    invariant(thenResult.expr);
    auto elseResult = generate(elseBranch);
    invariant(elseResult.expr);

    GenResult result;
    result.expr = makeE<EIf>(
        std::move(condResult.expr), std::move(thenResult.expr), std::move(elseResult.expr));
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const BinaryOp& op, const ABT& lhs, const ABT& rhs) {
    auto lhsResult = generate(lhs);
    invariant(lhsResult.expr);
    auto rhsResult = generate(rhs);
    invariant(rhsResult.expr);

    auto sbeOp = [](const BinaryOp::Op op) {
        switch (op) {
            case BinaryOp::logicAnd:
                return EPrimBinary::logicAnd;
            case BinaryOp::logicOr:
                return EPrimBinary::logicOr;
            default:;
        }
        MONGO_UNREACHABLE;
    }(op.op());

    GenResult result;
    result.expr = makeE<EPrimBinary>(sbeOp, std::move(lhsResult.expr), std::move(rhsResult.expr));
    return result;
}

ExeGenerator::GenResult ExeGenerator::walk(const UnaryOp& op, const ABT& arg) {
    auto argResult = generate(arg);
    invariant(argResult.expr);

    auto sbeOp = [](const UnaryOp::Op op) {
        switch (op) {
            case UnaryOp::logicNot:
                return EPrimUnary::logicNot;
            default:;
        }
        MONGO_UNREACHABLE;
    }(op.op());

    GenResult result;
    result.expr = makeE<EPrimUnary>(sbeOp, std::move(argResult.expr));
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const LocalBind& op, const ABT& bind, const ABT& in) {
    GenResult result;

    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const LambdaAbstraction& op,
                                           const ABT& param,
                                           const ABT& in) {
    auto binder = param.cast<ValueBinder>();
    auto it = _slots.find(binder);
    invariant(it != _slots.end());
    auto paramRes = generate(param);
    auto inRes = generate(in);
    GenResult result;
    result.expr =
        makeE<ELocalBind>(*it->second[0].frame, std::move(paramRes.exprs), std::move(inRes.expr));

    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const BoundParameter& op) {
    invariant(_lambdaCtx);
    GenResult result;
    result.expr = std::move(_lambdaCtx->at(op.position()));
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const OptFence& op, const ABT& arg) {
    return generate(arg);
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
