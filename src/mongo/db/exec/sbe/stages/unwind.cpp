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

#include "mongo/db/exec/sbe/stages/unwind.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
UnwindStage::UnwindStage(std::unique_ptr<PlanStage> input,
                         const std::string& inField,
                         const std::string& outField,
                         const std::string& outIndex)
    : _inField(inField), _outField(outField), _outIndex(outIndex) {
    _children.emplace_back(std::move(input));

    if (_outField == _outIndex) {
        uasserted(ErrorCodes::InternalError,
                  str::stream() << "duplicate field name: " << _outField);
    }
}
std::unique_ptr<PlanStage> UnwindStage::clone() {
    return std::make_unique<UnwindStage>(_children[0]->clone(), _inField, _outField, _outIndex);
}
void UnwindStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    // get the inField (incoming) accessor
    _inFieldAccessor = _children[0]->getAccessor(ctx, _inField);

    // prepare the outField output accessor
    _outFieldOutputAccessor = std::make_unique<value::ViewOfValueAccessor>();

    // prepare the outIndex output accessor
    _outIndexOutputAccessor = std::make_unique<value::ViewOfValueAccessor>();
}
value::SlotAccessor* UnwindStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (_outField == field) {
        return _outFieldOutputAccessor.get();
    }

    if (_outIndex == field) {
        return _outIndexOutputAccessor.get();
    }

    return _children[0]->getAccessor(ctx, field);
}
void UnwindStage::open(bool reOpen) {
    _children[0]->open(reOpen);

    _index = 0;
    _inArray = false;
}
PlanState UnwindStage::getNext() {
    if (!_inArray) {
        auto state = _children[0]->getNext();
        if (state != PlanState::ADVANCED) {
            return state;
        }

        // get the value
        auto [tag, val] = _inFieldAccessor->getViewOfValue();

        // TODO make it traverse BSON array
        if (tag != value::TypeTags::Array) {
            _outFieldOutputAccessor->reset(tag, val);
            _outIndexOutputAccessor->reset(value::TypeTags::Nothing, 0);
            return PlanState::ADVANCED;
        }

        _index = 0;
        _inArray = true;

        if (value::getArrayView(val)->size() == 0) {
            _inArray = false;
            _outFieldOutputAccessor->reset(value::TypeTags::Nothing, 0);
            _outIndexOutputAccessor->reset(value::TypeTags::Nothing, 0);
            return PlanState::ADVANCED;
        }
    }

    // in array
    auto [tag, val] = _inFieldAccessor->getViewOfValue();
    auto [tagElem, valElem] = value::getArrayView(val)->getAt(_index);

    _outFieldOutputAccessor->reset(tagElem, valElem);
    _outIndexOutputAccessor->reset(value::TypeTags::NumberInt64, _index);

    ++_index;
    if (_index == value::getArrayView(val)->size()) {
        _inArray = false;
    }

    return PlanState::ADVANCED;
}
void UnwindStage::close() {
    _children[0]->close();
}
std::vector<DebugPrinter::Block> UnwindStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "unwind");

    DebugPrinter::addIdentifier(ret, _outField);
    DebugPrinter::addIdentifier(ret, _outIndex);
    DebugPrinter::addIdentifier(ret, _inField);

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace sbe
}  // namespace mongo