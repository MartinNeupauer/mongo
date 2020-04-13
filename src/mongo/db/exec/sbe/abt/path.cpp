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
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
PathConstant::PathConstant(ABT c) : Base(std::move(c)) {
    checkValueSyntaxSort(get<0>());

    // TODO check the type
}

PathLambda::PathLambda(ABT c) : Base(std::move(c)) {
    checkValueSyntaxSort(get<0>());

    // TODO check the type
}

PathTraverse::PathTraverse(ABT c) : Base(std::move(c)) {
    checkPathSyntaxSort(get<0>());
}

PathField::PathField(std::string nameIn, ABT c) : Base(std::move(c)), _name(nameIn) {
    checkPathSyntaxSort(get<0>());
}

PathGet::PathGet(std::string nameIn, ABT c) : Base(std::move(c)), _name(nameIn) {
    checkPathSyntaxSort(get<0>());
}

PathCompose::PathCompose(ABT t2In, ABT t1In) : Base(std::move(t2In), std::move(t1In)) {
    checkPathSyntaxSort(get<0>());
    checkPathSyntaxSort(get<1>());
}

/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, PathTraverse& op, ABT* c) {
    mergeVarsHelper(&e, c);

    std::vector<Variable*> correlatedVars;

    if (auto it = _freeVars.find(&e); it != _freeVars.end()) {
        for (auto&& v : it->second) {
            correlatedVars.emplace_back(v.second);
        }
    }

    op.correlated() = correlatedVars;

    return &e;
}
/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walk(const PathIdentity& op) {
    return {};
}

ExeGenerator::GenResult ExeGenerator::walk(const PathConstant& op, const ABT& c) {
    auto resC = generate(c);
    invariant(resC.expr);

    _pathCtx->expr = std::move(resC.expr);
    auto inputSlot = _slotIdGen.generate();
    _currentStage =
        makeProjectStage(std::move(_currentStage), inputSlot, std::move(_pathCtx->expr));

    _pathCtx->expr = makeE<EVariable>(inputSlot);
    _pathCtx->slot = inputSlot;

    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathLambda& op, const ABT& lam) {
    invariant(_pathCtx->expr);

    std::vector<std::unique_ptr<EExpression>> localLambdaCtx;
    auto saveCtx = _lambdaCtx;
    _lambdaCtx = &localLambdaCtx;
    _lambdaCtx->emplace_back(std::move(_pathCtx->expr));

    auto resLam = generate(lam);
    invariant(resLam.expr);

    _pathCtx->expr = std::move(resLam.expr);
    auto inputSlot = _slotIdGen.generate();
    _currentStage =
        makeProjectStage(std::move(_currentStage), inputSlot, std::move(_pathCtx->expr));

    _pathCtx->expr = makeE<EVariable>(inputSlot);
    _pathCtx->slot = inputSlot;

    _lambdaCtx = saveCtx;
    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathDrop& op) {
    invariant(_pathCtx);

    _pathCtx->restrictFields.insert(
        _pathCtx->restrictFields.end(), op.names().begin(), op.names().end());
    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathKeep& op) {
    invariant(_pathCtx);
    GenResult result;

    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const PathObj& op) {
    invariant(_pathCtx);

    _pathCtx->forceNewObject = true;
    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathTraverse& op, const ABT& c) {
    invariant(_currentStage);
    value::SlotId inputSlot;
    if (!_pathCtx->slot) {
        inputSlot = _slotIdGen.generate();
        _currentStage =
            makeProjectStage(std::move(_currentStage), inputSlot, std::move(_pathCtx->expr));
    } else {
        inputSlot = *_pathCtx->slot;
    }
    value::SlotId outputSlot = _slotIdGen.generate();

    _pathCtx->expr = makeE<EVariable>(outputSlot);
    _pathCtx->slot = outputSlot;

    auto saveCurrent = std::move(_currentStage);
    auto saveCtx = _pathCtx;
    PathContext localPathCtx;
    _pathCtx = &localPathCtx;
    _pathCtx->topLevelTraverse = false;
    _pathCtx->expr = makeE<EVariable>(inputSlot);
    _pathCtx->slot = inputSlot;
    _pathCtx->inputMkObjSlot = inputSlot;
    _currentStage = makeS<LimitSkipStage>(makeS<CoScanStage>(), 1, boost::none);

    auto localRes = generate(c);
    if (!_pathCtx->expr) {
        generatePathMkObj();
    }
    auto resultExpr = std::move(_pathCtx->expr);
    auto resultSlot = _pathCtx->slot;
    auto resultStage = std::move(_currentStage);
    resultStage = makeProjectStage(std::move(resultStage), outputSlot, std::move(resultExpr));

    _pathCtx = saveCtx;
    _currentStage = std::move(saveCurrent);

    std::vector<value::SlotId> correlatedSlots;
    if (_pathCtx->topLevelTraverse) {
        std::unordered_set<value::SlotId> slots;
        for (auto& v : op.correlated()) {
            auto slot = getSlot(v->binding(), v->id());
            slots.insert(slot);
        }
        correlatedSlots = std::vector<value::SlotId>(slots.begin(), slots.end());
    }
    _currentStage = makeS<TraverseStage>(std::move(_currentStage),
                                         std::move(resultStage),
                                         inputSlot,
                                         outputSlot,
                                         outputSlot,
                                         correlatedSlots,
                                         nullptr,
                                         nullptr);

    _pathCtx->expr = makeE<EVariable>(outputSlot);
    _pathCtx->slot = outputSlot;

    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathField& op, const ABT& c) {
    // generate project to get the field value
    using namespace std::literals;
    invariant(_currentStage);
    auto localSlot = _slotIdGen.generate();
    _currentStage = makeProjectStage(
        std::move(_currentStage),
        localSlot,
        makeE<EFunction>("getField"sv,
                         makeEs(std::move(_pathCtx->expr), makeE<EConstant>(op.name()))));

    // new path context
    auto saveCtx = _pathCtx;
    PathContext localPathCtx;
    _pathCtx = &localPathCtx;
    _pathCtx->topLevelTraverse = saveCtx->topLevelTraverse;
    _pathCtx->expr = makeE<EVariable>(localSlot);
    _pathCtx->slot = localSlot;
    _pathCtx->inputMkObjSlot = localSlot;

    // generate c
    auto localRes = generate(c);
    // process results from c (aka MakeObj) got the field
    if (!_pathCtx->expr) {
        generatePathMkObj();
    }
    auto resultExpr = std::move(_pathCtx->expr);
    auto resultSlot = _pathCtx->slot;

    // pop path context
    _pathCtx = saveCtx;
    // set the field in context
    _pathCtx->projectFields.push_back(op.name());
    _pathCtx->projectVars.push_back(*resultSlot);
    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathGet& op, const ABT& c) {
    using namespace std::literals;
    invariant(_currentStage);
    auto localSlot = _slotIdGen.generate();
    _currentStage = makeProjectStage(
        std::move(_currentStage),
        localSlot,
        makeE<EFunction>("getField"sv,
                         makeEs(std::move(_pathCtx->expr), makeE<EConstant>(op.name()))));

    auto saveCtx = _pathCtx;
    PathContext localPathCtx;
    _pathCtx = &localPathCtx;
    _pathCtx->topLevelTraverse = saveCtx->topLevelTraverse;
    _pathCtx->expr = makeE<EVariable>(localSlot);
    _pathCtx->slot = localSlot;
    _pathCtx->inputMkObjSlot = localSlot;

    auto localRes = generate(c);
    if (!_pathCtx->expr) {
        generatePathMkObj();
    }
    auto resultExpr = std::move(_pathCtx->expr);
    auto resultSlot = _pathCtx->slot;

    _pathCtx = saveCtx;
    _pathCtx->expr = std::move(resultExpr);
    _pathCtx->slot = resultSlot;

    return {};
}
ExeGenerator::GenResult ExeGenerator::walk(const PathCompose& op, const ABT& t2, const ABT& t1) {
    invariant(_pathCtx->slot);
    auto savedSlot = *_pathCtx->slot;

    auto local1 = generate(t1);
    // somehow merge

    if (!_pathCtx->slot) {
        _pathCtx->slot = savedSlot;
    }
    if (!_pathCtx->expr) {
        _pathCtx->expr = makeE<EVariable>(*_pathCtx->slot);
    }
    auto local2 = generate(t2);
    // somehow merge

    return {};
}

}  // namespace abt
}  // namespace sbe
}  // namespace mongo
