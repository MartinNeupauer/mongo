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

#include "mongo/db/exec/sbe/stages/filter.h"

namespace mongo::sbe {

template class FilterStage<true>;
template class FilterStage<false>;

template <bool IsConst>
FilterStage<IsConst>::FilterStage(std::unique_ptr<PlanStage> input,
                                  std::unique_ptr<EExpression> filter)
    : _filter(std::move(filter)) {
    _children.emplace_back(std::move(input));
}
template <bool IsConst>
std::unique_ptr<PlanStage> FilterStage<IsConst>::clone() {
    return std::make_unique<FilterStage>(_children[0]->clone(), _filter->clone());
}
template <bool IsConst>
void FilterStage<IsConst>::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    // compile filter
    ctx.root = this;
    _filterCode = _filter->compile(ctx);
}
template <bool IsConst>
value::SlotAccessor* FilterStage<IsConst>::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    return _children[0]->getAccessor(ctx, slot);
}
template <bool IsConst>
void FilterStage<IsConst>::open(bool reOpen) {
    if constexpr (IsConst) {
        // run the filter expressions here
        auto [owned, tag, val] = _bytecode.run(_filterCode.get());
        auto pass = (tag == value::TypeTags::Boolean) && (val != 0);
        if (!pass) {
            close();
            return;
        }
    }
    _children[0]->open(reOpen);
    _childOpened = true;
}
template <bool IsConst>
PlanState FilterStage<IsConst>::getNext() {
    // The constant filter evaluates the predicate in the open method.
    if constexpr (IsConst) {
        if (!_childOpened) {
            return PlanState::IS_EOF;
        } else {
            return _children[0]->getNext();
        }
    }

    auto state = PlanState::IS_EOF;
    bool pass = false;

    do {
        state = _children[0]->getNext();

        if (state == PlanState::ADVANCED) {
            // run the filter expressions here
            auto [owned, tag, val] = _bytecode.run(_filterCode.get());
            pass = (tag == value::TypeTags::Boolean) && (val != 0);
        }
    } while (state == PlanState::ADVANCED && !pass);

    return state;
}
template <bool IsConst>
void FilterStage<IsConst>::close() {
    if (_childOpened) {
        _children[0]->close();

        _childOpened = false;
    }
}
template <bool IsConst>
std::vector<DebugPrinter::Block> FilterStage<IsConst>::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    if constexpr (IsConst) {
        DebugPrinter::addKeyword(ret, "cfilter");

    } else {
        DebugPrinter::addKeyword(ret, "filter");
    }

    ret.emplace_back("{`");
    DebugPrinter::addBlocks(ret, _filter->debugPrint());
    ret.emplace_back("`}");

    DebugPrinter::addNewLine(ret);

    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace mongo::sbe
