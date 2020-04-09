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

#include "mongo/db/exec/sbe/abt/exe_generator.h"
#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/stages.h"

namespace mongo {
namespace sbe {
namespace abt {
class SlotAllocator {
    ExeGenerator& _gen;

public:
    SlotAllocator(ExeGenerator& gen) : _gen(gen) {}
    void allocate(const ABT& root) {
        algebra::transport(root, *this);
    }
    template <typename... Ts>
    void transport(Ts&&...) {}

    void transport(const ValueBinder& op, const std::vector<ABT>& binds) {
        std::vector<ExeGenerator::SlotInfo> slots;
        for (const auto& b : binds) {
            ExeGenerator::SlotInfo info;
            auto& type = b.cast<ValueSyntaxSort>()->type();
            if (type.is<RowsetType>()) {
                // do nothing - rowsets plan stages are not slots
            } else {
                info.slot = _gen._slotIdGen.generate();
            }
            slots.emplace_back(std::move(info));
        }
        _gen._slots.emplace(&op, std::move(slots));
    }
    void transport(const LocalBind& op, const ABT& bind, const ABT& in) {
        auto it = _gen._slots.find(bind.cast<ValueBinder>());
        invariant(it != _gen._slots.end());
        auto& infos = it->second;

        auto frameId = _gen._frameIdGen.generate();
        for (size_t idx = 0; idx < infos.size(); ++idx) {
            infos[idx].frame = frameId;
            infos[idx].slot = idx;
        }
    }
    void transport(const LambdaAbstraction& op, const ABT& param, const ABT& in) {
        // This is conceptually wrong but because we are immediately executing lambdas
        // we can get away with this appoach - we are treating lambdas as let frames.
        auto it = _gen._slots.find(param.cast<ValueBinder>());
        invariant(it != _gen._slots.end());
        auto& infos = it->second;

        auto frameId = _gen._frameIdGen.generate();
        for (size_t idx = 0; idx < infos.size(); ++idx) {
            infos[idx].frame = frameId;
            infos[idx].slot = idx;
        }
    }
};
ExeGenerator::GenResult ExeGenerator::generate(const ABT& e) {
    return algebra::walk(e, *this);
}
ExeGenerator::GenResult ExeGenerator::generate() {
    SlotAllocator a(*this);

    a.allocate(_root);

    return generate(_root);
}
value::SlotId ExeGenerator::getSlot(const ValueBinder* binder, VarId id) {
    auto it = _slots.find(binder);
    invariant(it != _slots.end());

    auto& info = it->second[binder->index(id)];
    invariant(info.slot);

    return *info.slot;
}
ExeGenerator::GenResult ExeGenerator::generateInputPhase(VarId id, const ABT& body) {
    auto binder = body.cast<ValueBinder>();
    invariant(binder);

    auto it = _slots.find(binder);
    invariant(it != _slots.end());

    auto index = binder->index(id);

    for (size_t idx = 0; idx < index; ++idx) {
        if (binder->isUsed(binder->ids()[idx])) {
            invariant(_currentStage);
            auto localRes = generateBind(it->second[idx], binder->binds()[idx]);
            invariant(!_currentStage);
            _currentStage = std::move(localRes.stage);
        }
    }
    GenResult result;
    result.stage = std::move(_currentStage);
    return result;
}
ExeGenerator::GenResult ExeGenerator::generateOutputPhase(VarId id, const ABT& body) {
    auto binder = body.cast<ValueBinder>();
    invariant(binder);

    auto it = _slots.find(binder);
    invariant(it != _slots.end());

    auto index = binder->index(id);

    for (size_t idx = index; idx < binder->binds().size(); ++idx) {
        if (binder->isUsed(binder->ids()[idx])) {
            invariant(_currentStage);
            auto localRes = generateBind(it->second[idx], binder->binds()[idx]);
            invariant(!_currentStage);
            _currentStage = std::move(localRes.stage);
        }
    }

    GenResult result;
    result.stage = std::move(_currentStage);
    return result;
}
ExeGenerator::GenResult ExeGenerator::generateBind(const SlotInfo& info, const ABT& e) {
    auto bindResult = generate(e);
    if (!(bindResult.expr || bindResult.stage)) {
        GenResult result;
        result.stage = std::move(_currentStage);
        return result;
    }

    if (info.slot && info.frame) {
        // as expression
    } else {
        // as projection
        invariant(info.slot);
        if (bindResult.expr) {
            _currentStage =
                makeProjectStage(std::move(_currentStage), *info.slot, std::move(bindResult.expr));
        }
    }
    GenResult result;
    result.stage = std::move(_currentStage);
    result.expr = std::move(bindResult.expr);
    return result;
}
void ExeGenerator::generatePathMkObj() {
    invariant(_currentStage);
    invariant(_pathCtx);
    invariant(_pathCtx->inputMkObjSlot);

    auto outputSlot = _slotIdGen.generate();

    _currentStage = makeS<MakeObjStage>(std::move(_currentStage),
                                        outputSlot,
                                        *_pathCtx->inputMkObjSlot,
                                        _pathCtx->restrictFields,
                                        _pathCtx->projectFields,
                                        _pathCtx->projectVars,
                                        _pathCtx->forceNewObject,
                                        _pathCtx->returnOldObject);

    _pathCtx->expr = makeE<EVariable>(outputSlot);
    _pathCtx->slot = outputSlot;
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
