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

namespace mongo {
namespace sbe {
FilterStage::FilterStage(std::unique_ptr<PlanStage> input, std::unique_ptr<EExpression> filter)
    : _filter(std::move(filter)) {
    _children.emplace_back(std::move(input));
}
std::unique_ptr<PlanStage> FilterStage::clone() {
    return std::make_unique<FilterStage>(_children[0]->clone(), _filter->clone());
}
void FilterStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    // compile filter
    ctx.root = this;
    _filterCode = _filter->compile(ctx);
}
value::SlotAccessor* FilterStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    return _children[0]->getAccessor(ctx, field);
}
void FilterStage::open(bool reOpen) {
    _children[0]->open(reOpen);
}
PlanState FilterStage::getNext() {
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
void FilterStage::close() {
    _children[0]->close();
}
std::vector<DebugPrinter::Block> FilterStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "filter");

    ret.emplace_back("{`");
    DebugPrinter::addBlocks(ret, _filter->debugPrint());
    ret.emplace_back("`}");

    DebugPrinter::addNewLine(ret);

    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace sbe
}  // namespace mongo