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

#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo::sbe {
TraverseStage::TraverseStage(std::unique_ptr<PlanStage> outer,
                             std::unique_ptr<PlanStage> inner,
                             value::SlotId inField,
                             value::SlotId outField,
                             value::SlotId outFieldInner,
                             std::unique_ptr<EExpression> foldExpr,
                             std::unique_ptr<EExpression> finalExpr)
    : _inField(inField),
      _outField(outField),
      _outFieldInner(outFieldInner),
      _fold(std::move(foldExpr)),
      _final(std::move(finalExpr)) {
    _children.emplace_back(std::move(outer));
    _children.emplace_back(std::move(inner));

    if (_inField == _outField && (_fold || _final)) {
        uasserted(ErrorCodes::InternalError, "in and out field must not match when folding");
    }
}
std::unique_ptr<PlanStage> TraverseStage::clone() {
    return std::make_unique<TraverseStage>(_children[0]->clone(),
                                           _children[1]->clone(),
                                           _inField,
                                           _outField,
                                           _outFieldInner,
                                           _fold ? _fold->clone() : nullptr,
                                           _final ? _final->clone() : nullptr);
}
void TraverseStage::prepare(CompileCtx& ctx) {
    // prepare the outer side as usual
    _children[0]->prepare(ctx);

    // get the inField (incoming) accessor
    _inFieldAccessor = _children[0]->getAccessor(ctx, _inField);

    // prepare the accessor for the correlated parameter
    _correlatedAccessor = std::make_unique<value::ViewOfValueAccessor>();
    ctx.pushCorrelated(_inField, _correlatedAccessor.get());

    // prepare the outField output accessor (this accumulates values from the inner side to an
    // array)
    _outFieldOutputAccessor = std::make_unique<value::OwnedValueAccessor>();

    // prepare the inner side
    _children[1]->prepare(ctx);

    // get the output from the inner side
    _outFieldInputAccessor = _children[1]->getAccessor(ctx, /*_inField*/ _outFieldInner);

    if (_fold) {
        ctx.root = this;
        _foldCode = _fold->compile(ctx);
    }

    if (_final) {
        ctx.root = this;
        _finalCode = _final->compile(ctx);
    }

    // restore correlated parameters
    ctx.popCorrelated();

    _compiled = true;
}
value::SlotAccessor* TraverseStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_outField == slot) {
        return _outFieldOutputAccessor.get();
    }

    if (_compiled) {
        // after the compilation pass to the 'from' child
        return _children[0]->getAccessor(ctx, slot);
    } else {
        // if internal expressions (fold, final) are not compiled yet then they refer to the 'in'
        // child
        return _children[1]->getAccessor(ctx, slot);
    }
}
void TraverseStage::open(bool reOpen) {
    _children[0]->open(reOpen);
    // do not open the inner child as we do not have values of correlated parameters yet.
    // the values are available only after we call getNext on the outer side.
}

void TraverseStage::openInner(value::TypeTags tag, value::Value val) {
    // set the correlated value
    _correlatedAccessor->reset(tag, val);

    // and (re)open the inner side as it can see the correlated value now
    _children[1]->open(_reOpenInner);
    _reOpenInner = true;
}

PlanState TraverseStage::getNext() {
    auto state = _children[0]->getNext();
    if (state != PlanState::ADVANCED) {
        return state;
    }

    // get the value
    auto [tag, val] = _inFieldAccessor->getViewOfValue();

    if (value::isArray(tag)) {
        // if it is an array then we have to traverse it
        _inArrayAccessor.reset(tag, val);
        value::Array* arrOut{nullptr};

        if (!_foldCode) {
            // create a fresh new output array
            // TODO if _inField == _outField then we can do implace update of the input array
            auto [tag, val] = value::makeNewArray();
            arrOut = value::getArrayView(val);
            _outFieldOutputAccessor->reset(true, tag, val);
        } else {
            _outFieldOutputAccessor->reset(false, value::TypeTags::Nothing, 0);
        }

        // loop over all elements of array
        // TODO - implement a stack of nested arrays
        bool firstValue = true;
        for (; !_inArrayAccessor.atEnd(); _inArrayAccessor.advance()) {
            auto [tag, val] = _inArrayAccessor.getViewOfValue();

            // execute inner side once for every element of the array
            openInner(tag, val);
            auto state = _children[1]->getNext();

            if (state == PlanState::ADVANCED) {
                if (!_foldCode) {
                    // we have to copy (or move optimization) the value to the array
                    // as by definition all composite values (arrays, objects) own their
                    // constituents
                    auto [tag, val] = _outFieldInputAccessor->copyOrMoveValue();
                    arrOut->push_back(tag, val);
                } else {
                    if (firstValue) {
                        auto [tag, val] = _outFieldInputAccessor->copyOrMoveValue();
                        _outFieldOutputAccessor->reset(true, tag, val);
                        firstValue = false;
                    } else {
                        // fold
                        auto [owned, tag, val] = _bytecode.run(_foldCode.get());
                        if (!owned) {
                            auto [copyTag, copyVal] = value::copyValue(tag, val);
                            tag = copyTag;
                            val = copyVal;
                        }
                        _outFieldOutputAccessor->reset(true, tag, val);
                    }
                }

                // check early out condition
                if (_finalCode) {
                    auto [owned, tag, val] = _bytecode.run(_finalCode.get());
                    if (tag == value::TypeTags::Boolean && val != 0) {
                        break;
                    }
                }
            }
        }
    } else {
        // for non-arrays we simply execute the inner side once
        openInner(tag, val);
        auto state = _children[1]->getNext();

        if (state == PlanState::IS_EOF) {
            _outFieldOutputAccessor->reset();
        } else {
            auto [tag, val] = _outFieldInputAccessor->getViewOfValue();
            // we don't have to copy the value
            _outFieldOutputAccessor->reset(false, tag, val);
        }
    }

    return PlanState::ADVANCED;
}
void TraverseStage::close() {
    if (_reOpenInner) {
        _children[1]->close();

        _reOpenInner = false;
    }

    _children[0]->close();
}
std::vector<DebugPrinter::Block> TraverseStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "traverse");

    DebugPrinter::addIdentifier(ret, _outField);
    DebugPrinter::addIdentifier(ret, _outFieldInner);
    DebugPrinter::addIdentifier(ret, _inField);

    if (_fold) {
        ret.emplace_back("{`");
        DebugPrinter::addBlocks(ret, _fold->debugPrint());
        ret.emplace_back("`}");
    }

    if (_final) {
        ret.emplace_back("{`");
        DebugPrinter::addBlocks(ret, _final->debugPrint());
        ret.emplace_back("`}");
    }

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addIdentifier(ret, "in");
    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);
    DebugPrinter::addBlocks(ret, _children[1]->debugPrint());
    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);

    DebugPrinter::addIdentifier(ret, "from");
    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());
    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);

    return ret;
}
}  // namespace mongo::sbe
