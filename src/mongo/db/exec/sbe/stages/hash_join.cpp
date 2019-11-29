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

#include "mongo/db/exec/sbe/stages/hash_join.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
HashJoinStage::HashJoinStage(std::unique_ptr<PlanStage> outer,
                             std::unique_ptr<PlanStage> inner,
                             const std::vector<std::string>& outerCond,
                             const std::vector<std::string>& outerProjects,
                             const std::vector<std::string>& innerCond,
                             const std::vector<std::string>& innerProjects)
    : _outerCond(outerCond),
      _outerProjects(outerProjects),
      _innerCond(innerCond),
      _innerProjects(innerProjects)  // DEAD CODE !!!!
{
    if (_outerCond.size() != _innerCond.size()) {
        uasserted(ErrorCodes::InternalError, "left and right size do not match");
    }

    _children.emplace_back(std::move(outer));
    _children.emplace_back(std::move(inner));
}
std::unique_ptr<PlanStage> HashJoinStage::clone() {
    return std::make_unique<HashJoinStage>(_children[0]->clone(),
                                           _children[1]->clone(),
                                           _outerCond,
                                           _outerProjects,
                                           _innerCond,
                                           _innerProjects);
}
void HashJoinStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);
    _children[1]->prepare(ctx);

    size_t counter = 0;
    for (auto& p : _outerCond) {
        auto [it, inserted] = _inOuterKeyAccessors.emplace(p, _children[0]->getAccessor(ctx, p));
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << p, inserted);

        _outOuterKeyAccessors.emplace_back(std::make_unique<HashKeyAccessor>(_htIt, counter++));
        _outOuterAccessors[p] = _outOuterKeyAccessors.back().get();
    }

    counter = 0;
    for (auto& p : _innerCond) {
        auto [it, inserted] = _inInnerKeyAccessors.emplace(p, _children[1]->getAccessor(ctx, p));
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << p, inserted);
    }

    counter = 0;
    for (auto& p : _outerProjects) {
        auto [it, inserted] =
            _inOuterProjectAccessors.emplace(p, _children[0]->getAccessor(ctx, p));
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << p, inserted);

        _outOuterProjectAccessors.emplace_back(
            std::make_unique<HashProjectAccessor>(_htIt, counter++));
        _outOuterAccessors[p] = _outOuterProjectAccessors.back().get();
    }

    _compiled = true;
}

value::SlotAccessor* HashJoinStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (_compiled) {
        if (auto it = _outOuterAccessors.find(field); it != _outOuterAccessors.end()) {
            return it->second;
        }

        return _children[1]->getAccessor(ctx, field);
    }

    return ctx.getAccessor(field);
}
void HashJoinStage::open(bool reOpen) {
    _children[0]->open(reOpen);
    // insert the outer side into the hash table
    value::MaterializedRow key;
    value::MaterializedRow project;

    while (_children[0]->getNext() == PlanState::ADVANCED) {
        key._fields.reserve(_inOuterKeyAccessors.size());
        project._fields.reserve(_inOuterProjectAccessors.size());

        // copy keys in order to do the lookup
        for (auto& p : _inOuterKeyAccessors) {
            key._fields.push_back(value::OwnedValueAccessor{});
            auto [tag, val] = p.second->copyOrMoveValue();
            key._fields.back().reset(true, tag, val);
        }

        // copy projects
        for (auto& p : _inOuterProjectAccessors) {
            project._fields.push_back(value::OwnedValueAccessor{});
            auto [tag, val] = p.second->copyOrMoveValue();
            project._fields.back().reset(true, tag, val);
        }

        auto it = _ht.emplace(std::move(key), std::move(project));
    }

    _children[0]->close();

    _children[1]->open(reOpen);

    _htIt = _ht.end();
    _htItEnd = _ht.end();
}
PlanState HashJoinStage::getNext() {
    if (_htIt != _htItEnd) {
        ++_htIt;
    }

    if (_htIt == _htItEnd) {
        while (_htIt == _htItEnd) {
            auto state = _children[1]->getNext();
            if (state == PlanState::IS_EOF) {
                // LEFT and OUTER joins should enumerate "non-returned" rows here
                return state;
            }
            value::MaterializedRow key;
            key._fields.reserve(_inInnerKeyAccessors.size());
            // copy keys in order to do the lookup
            for (auto& p : _inInnerKeyAccessors) {
                key._fields.push_back(value::OwnedValueAccessor{});
                auto [tag, val] = p.second->getViewOfValue();
                key._fields.back().reset(false, tag, val);
            }

            auto [low, hi] = _ht.equal_range(key);
            _htIt = low;
            _htItEnd = hi;
            // if _htIt == _htItEnd (i.e. no match) then RIGHT and OUTER joins
            // should enumerate "non-returned" rows here
        }
    }

    return PlanState::ADVANCED;
}
void HashJoinStage::close() {
    _children[1]->close();
}
std::vector<DebugPrinter::Block> HashJoinStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "hj");

    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);

    DebugPrinter::addKeyword(ret, "left");

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _outerCond.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _outerCond[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _outerProjects.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _outerProjects[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());
    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);

    DebugPrinter::addKeyword(ret, "right");
    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _innerCond.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _innerCond[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _innerProjects.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _innerProjects[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);
    DebugPrinter::addBlocks(ret, _children[1]->debugPrint());
    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);

    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);

    return ret;
}
}  // namespace sbe
}  // namespace mongo