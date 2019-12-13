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

#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
LoopJoinStage::LoopJoinStage(std::unique_ptr<PlanStage> outer,
                             std::unique_ptr<PlanStage> inner,
                             const std::vector<std::string>& outerProjects,
                             const std::vector<std::string> outerCorrelated,
                             std::unique_ptr<EExpression> predicate)
    : _outerProjects(outerProjects),
      _outerCorrelated(outerCorrelated),
      _predicate(std::move(predicate)) {
    _children.emplace_back(std::move(outer));
    _children.emplace_back(std::move(inner));
}
std::unique_ptr<PlanStage> LoopJoinStage::clone() {
    return std::make_unique<LoopJoinStage>(_children[0]->clone(),
                                           _children[1]->clone(),
                                           _outerProjects,
                                           _outerCorrelated,
                                           _predicate->clone());
}
void LoopJoinStage::prepare(CompileCtx& ctx) {
    for (auto& f : _outerProjects) {
        auto [it, inserted] = _outerRefs.emplace(f);
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << f, inserted);
    }
    _children[0]->prepare(ctx);

    for (auto& f : _outerCorrelated) {
        ctx.pushCorrelated(f, _children[0]->getAccessor(ctx, f));
    }
    _children[1]->prepare(ctx);

    for (size_t idx = 0; idx < _outerCorrelated.size(); ++idx) {
        ctx.popCorrelated();
    }

    if (_predicate) {
        ctx.root = this;
        _predicateCode = _predicate->compile(ctx);
    }
}
value::SlotAccessor* LoopJoinStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (_outerRefs.count(field)) {
        return _children[0]->getAccessor(ctx, field);
    }

    return _children[1]->getAccessor(ctx, field);
}
void LoopJoinStage::open(bool reOpen) {
    _children[0]->open(reOpen);
    _outerGetNext = true;
    // do not open the inner child as we do not have values of correlated parameters yet.
    // the values are available only after we call getNext on the outer side.
}
void LoopJoinStage::openInner() {
    // (re)open the inner side as it can see the correlated value now
    _children[1]->open(_reOpenInner);
    _reOpenInner = true;
}
PlanState LoopJoinStage::getNext() {
    if (_outerGetNext) {
        auto state = _children[0]->getNext();
        if (state != PlanState::ADVANCED) {
            return state;
        }

        openInner();
        _outerGetNext = false;
    }

    for (;;) {
        auto state = PlanState::IS_EOF;
        bool pass = false;

        do {

            state = _children[1]->getNext();
            if (state == PlanState::ADVANCED) {
                if (!_predicateCode) {
                    pass = true;
                } else {
                    auto [owned, tag, val] = _bytecode.run(_predicateCode.get());
                    pass = (tag == value::TypeTags::Boolean) && (val != 0);
                }
            }
        } while (state == PlanState::ADVANCED && !pass);

        if (state == PlanState::ADVANCED) {
            return PlanState::ADVANCED;
        }
        invariant(state == PlanState::IS_EOF);

        state = _children[0]->getNext();
        if (state != PlanState::ADVANCED) {
            return state;
        }

        openInner();
    }
}
void LoopJoinStage::close() {
    if (_reOpenInner) {
        _children[1]->close();

        _reOpenInner = false;
    }

    _children[0]->close();
}
std::vector<DebugPrinter::Block> LoopJoinStage::debugPrint() {
    return std::vector<DebugPrinter::Block>();
}
}  // namespace sbe
}  // namespace mongo