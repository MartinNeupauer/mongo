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

#include "mongo/db/exec/sbe/stages/sort.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/util/str.h"

#include <set>

namespace mongo {
namespace sbe {
SortStage::SortStage(std::unique_ptr<PlanStage> input,
                     const std::vector<std::string>& obs,
                     const std::vector<std::string>& vals)
    : _obs(obs), _vals(vals) {
    _children.emplace_back(std::move(input));
}
std::unique_ptr<PlanStage> SortStage::clone() {
    return std::make_unique<SortStage>(_children[0]->clone(), _obs, _vals);
}
void SortStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    std::set<std::string> dupCheck;

    size_t counter = 0;
    // process order by fields
    for (auto& name : _obs) {
        auto [it, inserted] = dupCheck.insert(name);
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << name, inserted);

        _inKeyAccessors.emplace_back(_children[0]->getAccessor(ctx, name));
        _outAccessors.emplace(name, std::make_unique<SortKeyAccessor>(_stIt, counter++));
    }

    counter = 0;
    // process value fields
    for (auto& name : _vals) {
        auto [it, inserted] = dupCheck.insert(name);
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << name, inserted);

        _inValueAccessors.emplace_back(_children[0]->getAccessor(ctx, name));
        _outAccessors.emplace(name, std::make_unique<SortValueAccessor>(_stIt, counter++));
    }
}
value::SlotAccessor* SortStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (auto it = _outAccessors.find(field); it != _outAccessors.end()) {
        return it->second.get();
    }

    return ctx.getAccessor(field);
}
void SortStage::open(bool reOpen) {
    _children[0]->open(reOpen);

    value::MaterializedRow keys;
    value::MaterializedRow vals;
    keys._fields.reserve(_inKeyAccessors.size());
    vals._fields.reserve(_inValueAccessors.size());

    while (_children[0]->getNext() == PlanState::ADVANCED) {
        for (auto accesor : _inKeyAccessors) {
            keys._fields.push_back(value::OwnedValueAccessor{});
            auto [tag, val] = accesor->copyOrMoveValue();
            keys._fields.back().reset(true, tag, val);
        }
        for (auto accesor : _inValueAccessors) {
            vals._fields.push_back(value::OwnedValueAccessor{});
            auto [tag, val] = accesor->copyOrMoveValue();
            vals._fields.back().reset(true, tag, val);
        }

        _st.emplace(std::move(keys), std::move(vals));
    }

    _children[0]->close();

    _stIt = _st.end();
}
PlanState SortStage::getNext() {
    if (_stIt == _st.end()) {
        _stIt = _st.begin();
    } else {
        ++_stIt;
    }

    if (_stIt == _st.end()) {
        return PlanState::IS_EOF;
    }

    return PlanState::ADVANCED;
}
void SortStage::close() {}
std::vector<DebugPrinter::Block> SortStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "sort");

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _obs.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _obs[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _vals.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _vals[idx]);
    }
    ret.emplace_back("`]");

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace sbe
}  // namespace mongo