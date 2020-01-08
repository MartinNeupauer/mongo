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

#include "mongo/db/exec/sbe/stages/limit.h"

namespace mongo::sbe {
LimitStage::LimitStage(std::unique_ptr<PlanStage> input, int limit) : _limit(limit), _current(0) {
    _children.emplace_back(std::move(input));
}

std::unique_ptr<PlanStage> LimitStage::clone() {
    return std::make_unique<LimitStage>(_children[0]->clone(), _limit);
}

void LimitStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);
}

value::SlotAccessor* LimitStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    return _children[0]->getAccessor(ctx, slot);
}
void LimitStage::open(bool reOpen) {
    _current = 0;
    _children[0]->open(reOpen);
}
PlanState LimitStage::getNext() {
    if (_current++ == _limit) {
        return PlanState::IS_EOF;
    }

    return _children[0]->getNext();
}
void LimitStage::close() {
    _children[0]->close();
}
std::vector<DebugPrinter::Block> LimitStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "limit");

    ret.emplace_back(std::to_string(_limit));
    DebugPrinter::addNewLine(ret);

    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace mongo::sbe
