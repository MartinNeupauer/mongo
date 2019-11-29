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

#include "mongo/db/exec/sbe/stages/hash_agg.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
HashAggStage::HashAggStage(std::unique_ptr<PlanStage> input,
                           const std::vector<std::string>& gbs,
                           std::unordered_map<std::string, std::unique_ptr<EExpression>> aggs)
    : _gbs(gbs), _aggs(std::move(aggs)) {
    _children.emplace_back(std::move(input));
}
std::unique_ptr<PlanStage> HashAggStage::clone() {
    std::unordered_map<std::string, std::unique_ptr<EExpression>> aggs;
    for (auto& [k, v] : _aggs) {
        aggs.emplace(k, v->clone());
    }
    return std::make_unique<HashAggStage>(_children[0]->clone(), _gbs, std::move(aggs));
}
void HashAggStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    size_t counter = 0;
    // process group by columns
    for (auto& name : _gbs) {
        auto [it, inserted] = _inKeyAccessors.emplace(name, _children[0]->getAccessor(ctx, name));
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << name, inserted);

        _outKeyAccessors.emplace_back(std::make_unique<HashKeyAccessor>(_htIt, counter++));
        _outAccessors[name] = _outKeyAccessors.back().get();
    }

    counter = 0;
    for (auto& [name, expr] : _aggs) {
        _outAggAccessors.emplace_back(std::make_unique<HashAggAccessor>(_htIt, counter++));
        uassert(ErrorCodes::InternalError,
                str::stream() << "duplicate field: " << name,
                !_outAccessors.count(name));
        _outAccessors[name] = _outAggAccessors.back().get();

        ctx.root = this;
        ctx.aggExpression = true;
        ctx.accumulator = _outAggAccessors.back().get();

        _aggCodes.emplace_back(expr->compile(ctx));
        ctx.aggExpression = false;
    }
    _compiled = true;
}
value::SlotAccessor* HashAggStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (_compiled) {
        if (auto it = _outAccessors.find(field); it != _outAccessors.end()) {
            return it->second;
        }
    } else {
        return _children[0]->getAccessor(ctx, field);
    }

    return ctx.getAccessor(field);
}
void HashAggStage::open(bool reOpen) {
    _children[0]->open(reOpen);

    value::MaterializedRow key;
    value::MaterializedRow agg;
    key._fields.reserve(_inKeyAccessors.size());
    while (_children[0]->getNext() == PlanState::ADVANCED) {
        // copy keys in order to do the lookup
        for (auto& p : _inKeyAccessors) {
            key._fields.push_back(value::OwnedValueAccessor{});
            auto [tag, val] = p.second->getViewOfValue();
            key._fields.back().reset(false, tag, val);
        }

        auto [it, inserted] = _ht.emplace(std::move(key), std::move(agg));
        if (inserted) {
            // copy keys
            const_cast<value::MaterializedRow&>(it->first).makeOwned();
            // initialize accumulators
            it->second._fields.resize(_outAggAccessors.size());
        }
        // accumulate
        _htIt = it;
        for (size_t idx = 0; idx < _outAggAccessors.size(); ++idx) {
            auto [owned, tag, val] = _bytecode.run(_aggCodes[idx].get());
            _outAggAccessors[idx]->reset(owned, tag, val);
        }

        key._fields.clear();
    }

    _children[0]->close();

    _htIt = _ht.end();
}
PlanState HashAggStage::getNext() {
    if (_htIt == _ht.end()) {
        _htIt = _ht.begin();
    } else {
        ++_htIt;
    }

    if (_htIt == _ht.end()) {
        return PlanState::IS_EOF;
    }

    return PlanState::ADVANCED;
}

void HashAggStage::close() {}
std::vector<DebugPrinter::Block> HashAggStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "group");

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _gbs.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _gbs[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block("[`"));
    bool first = true;
    for (auto& p : _aggs) {
        if (!first) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, p.first);
        ret.emplace_back("=");
        DebugPrinter::addBlocks(ret, p.second->debugPrint());
        first = false;
    }
    ret.emplace_back("`]");

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace sbe
}  // namespace mongo
