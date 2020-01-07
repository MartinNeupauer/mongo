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

#pragma once

#include "mongo/db/exec/sbe/values/value.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace mongo {
namespace sbe {
namespace vm {
template <typename Op>
std::pair<value::TypeTags, value::Value> genericNumericCompare(value::TypeTags lhsTag,
                                                               value::Value lhsValue,
                                                               value::TypeTags rhsTag,
                                                               value::Value rhsValue,
                                                               Op op) {

    if (value::isNumber(lhsTag) && value::isNumber(rhsTag)) {
        switch (getWidestNumericalType(lhsTag, rhsTag)) {
            case value::TypeTags::NumberInt32: {
                auto result = op(value::numericCast<int32_t>(lhsTag, lhsValue),
                                 value::numericCast<int32_t>(rhsTag, rhsValue));
                return {value::TypeTags::Boolean, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberInt64: {
                auto result = op(value::numericCast<int64_t>(lhsTag, lhsValue),
                                 value::numericCast<int64_t>(rhsTag, rhsValue));
                return {value::TypeTags::Boolean, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberDouble: {
                auto result = op(value::numericCast<double>(lhsTag, lhsValue),
                                 value::numericCast<double>(rhsTag, rhsValue));
                return {value::TypeTags::Boolean, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberDecimal: {
                auto result = op(value::numericCast<Decimal128>(lhsTag, lhsValue),
                                 value::numericCast<Decimal128>(rhsTag, rhsValue));
                return {value::TypeTags::Boolean, value::bitcastFrom(result)};
            }
            default:
                MONGO_UNREACHABLE;
        }
    }

    return {value::TypeTags::Nothing, 0};
}

struct Instruction {
    enum Tags {
        pushConstVal,
        pushAccessVal,

        add,

        less,
        lessEq,
        greater,
        greaterEq,
        eq,

        getField,

        sum,

        exists,
        isObject,

        function,

        jmp,  // offset is calculated from the end of instruction
        jmpTrue,
        jmpNothing,
    };
    uint8_t owned : 1;
    uint8_t tag : 7;
};
static_assert(sizeof(Instruction) == sizeof(uint8_t));

enum class Builtin : uint8_t {
    split,
    dropFields,
    newObj,
    ksToString,  // KeyString to string
    newKs,       // new KeyString
};

class CodeFragment {
    std::vector<uint8_t> _instrs;

    auto allocateSpace(size_t size) {
        auto oldSize = _instrs.size();
        _instrs.resize(oldSize + size);
        return _instrs.data() + oldSize;
    }

public:
    auto& instrs() {
        return _instrs;
    }
    void append(std::unique_ptr<CodeFragment> code);
    void appendConstVal(value::TypeTags tag, value::Value val, bool owned = false);
    void appendAccessVal(value::SlotAccessor* accessor);
    void appendAdd();
    void appendLess(bool owned = false) {
        appendComparison(Instruction::less, owned);
    }
    void appendLessEq(bool owned = false) {
        appendComparison(Instruction::lessEq, owned);
    }
    void appendGreater(bool owned = false) {
        appendComparison(Instruction::greater, owned);
    }
    void appendGreaterEq(bool owned = false) {
        appendComparison(Instruction::greaterEq, owned);
    }
    void appendEq(bool owned = false) {
        appendComparison(Instruction::eq, owned);
    }
    void appendGetField();
    void appendSum(bool owned = false);
    void appendExists();
    void appendIsObject();
    void appendFunction(Builtin f, uint8_t arity);
    void appendJump(int jumpOffset);
    void appendJumpTrue(int jumpOffset);
    void appendJumpNothing(int jumpOffset);

private:
    void appendComparison(Instruction::Tags tag, bool owned);
};

class ByteCode {
    std::vector<uint8_t> _argStackOwned;
    std::vector<value::TypeTags> _argStackTags;
    std::vector<value::Value> _argStackVals;

    std::tuple<bool, value::TypeTags, value::Value> genericAdd(value::TypeTags lhsTag,
                                                               value::Value lhsValue,
                                                               value::TypeTags rhsTag,
                                                               value::Value rhsValue);
    template <typename Op>
    std::pair<value::TypeTags, value::Value> genericCompare(value::TypeTags lhsTag,
                                                            value::Value lhsValue,
                                                            value::TypeTags rhsTag,
                                                            value::Value rhsValue,
                                                            Op op = {}) {
        return genericNumericCompare(lhsTag, lhsValue, rhsTag, rhsValue, op);
    }

    std::pair<value::TypeTags, value::Value> genericCompareEq(value::TypeTags lhsTag,
                                                              value::Value lhsValue,
                                                              value::TypeTags rhsTag,
                                                              value::Value rhsValue);

    std::tuple<bool, value::TypeTags, value::Value> getField(value::TypeTags objTag,
                                                             value::Value objValue,
                                                             value::TypeTags fieldTag,
                                                             value::Value fieldValue);

    std::pair<value::TypeTags, value::Value> aggSum(value::TypeTags accTag,
                                                    value::Value accValue,
                                                    value::TypeTags fieldTag,
                                                    value::Value fieldValue);

    std::tuple<bool, value::TypeTags, value::Value> builtinSplit(uint8_t arity);
    std::tuple<bool, value::TypeTags, value::Value> builtinDropFields(uint8_t arity);
    std::tuple<bool, value::TypeTags, value::Value> builtinNewObj(uint8_t arity);
    std::tuple<bool, value::TypeTags, value::Value> builtinKeyStringToString(uint8_t arity);
    std::tuple<bool, value::TypeTags, value::Value> builtinNewKeyString(uint8_t arity);

    std::tuple<bool, value::TypeTags, value::Value> dispatchBuiltin(Builtin f, uint8_t arity);

    std::tuple<bool, value::TypeTags, value::Value> getFromStack(size_t offset) {
        auto backOffset = _argStackOwned.size() - 1 - offset;
        auto owned = _argStackOwned[backOffset];
        auto tag = _argStackTags[backOffset];
        auto val = _argStackVals[backOffset];

        return {owned, tag, val};
    }

    void pushStack(bool owned, value::TypeTags tag, value::Value val) {
        _argStackOwned.push_back(owned);
        _argStackTags.push_back(tag);
        _argStackVals.push_back(val);
    }

    void topStack(bool owned, value::TypeTags tag, value::Value val) {
        _argStackOwned.back() = owned;
        _argStackTags.back() = tag;
        _argStackVals.back() = val;
    }

    void popStack() {
        _argStackOwned.pop_back();
        _argStackTags.pop_back();
        _argStackVals.pop_back();
    }

public:
    std::tuple<uint8_t, value::TypeTags, value::Value> run(CodeFragment* code);
};
}  // namespace vm
}  // namespace sbe
}  // namespace mongo
