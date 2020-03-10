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

#include "mongo/db/exec/sbe/vm/vm.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/storage/key_string.h"

#include <set>

namespace mongo {
namespace sbe {
namespace vm {
void CodeFragment::append(std::unique_ptr<CodeFragment> code) {
    _instrs.insert(_instrs.end(), code->_instrs.begin(), code->_instrs.end());
}

void CodeFragment::appendConstVal(value::TypeTags tag, value::Value val, bool owned) {
    Instruction i;
    i.owned = owned;
    i.tag = Instruction::pushConstVal;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(tag) + sizeof(val));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, tag);
    offset += value::writeToMemory(offset, val);
}

void CodeFragment::appendAccessVal(value::SlotAccessor* accessor) {
    Instruction i;
    i.owned = false;
    i.tag = Instruction::pushAccessVal;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(accessor));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, accessor);
}

void CodeFragment::appendAdd() {
    appendSimpleInstruction(Instruction::add);
}
void CodeFragment::appendSub() {
    appendSimpleInstruction(Instruction::sub);
}
void CodeFragment::appendMul() {
    appendSimpleInstruction(Instruction::mul);
}
void CodeFragment::appendDiv() {
    appendSimpleInstruction(Instruction::div);
}
void CodeFragment::appendNegate() {
    appendSimpleInstruction(Instruction::negate);
}
void CodeFragment::appendNot() {
    appendSimpleInstruction(Instruction::logicNot);
}
void CodeFragment::appendSimpleInstruction(Instruction::Tags tag) {
    Instruction i;
    i.owned = false;  // this is not used
    i.tag = tag;

    auto offset = allocateSpace(sizeof(Instruction));

    offset += value::writeToMemory(offset, i);
}
void CodeFragment::appendGetField() {
    appendSimpleInstruction(Instruction::getField);
}
void CodeFragment::appendSum() {
    appendSimpleInstruction(Instruction::sum);
}
void CodeFragment::appendExists() {
    appendSimpleInstruction(Instruction::exists);
}
void CodeFragment::appendIsObject() {
    appendSimpleInstruction(Instruction::isObject);
}
void CodeFragment::appendFunction(Builtin f, uint8_t arity) {
    Instruction i;
    i.owned = false;  // this is not used
    i.tag = Instruction::function;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(f) + sizeof(arity));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, f);
    offset += value::writeToMemory(offset, arity);
}
void CodeFragment::appendJump(int jumpOffset) {
    Instruction i;
    i.owned = false;  // this is not used
    i.tag = Instruction::jmp;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(jumpOffset));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, jumpOffset);
}
void CodeFragment::appendJumpTrue(int jumpOffset) {
    Instruction i;
    i.owned = false;  // this is not used
    i.tag = Instruction::jmpTrue;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(jumpOffset));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, jumpOffset);
}
void CodeFragment::appendJumpNothing(int jumpOffset) {
    Instruction i;
    i.owned = false;  // this is not used
    i.tag = Instruction::jmpNothing;

    auto offset = allocateSpace(sizeof(Instruction) + sizeof(jumpOffset));

    offset += value::writeToMemory(offset, i);
    offset += value::writeToMemory(offset, jumpOffset);
}

std::tuple<bool, value::TypeTags, value::Value> ByteCode::getField(value::TypeTags objTag,
                                                                   value::Value objValue,
                                                                   value::TypeTags fieldTag,
                                                                   value::Value fieldValue) {
    if (!value::isString(fieldTag)) {
        return {false, value::TypeTags::Nothing, 0};
    }

    auto fieldStr = value::getStringView(fieldTag, fieldValue);

    if (objTag == value::TypeTags::Object) {
        auto [tag, val] = value::getObjectView(objValue)->getField(fieldStr);
        return {false, tag, val};
    } else if (objTag == value::TypeTags::bsonObject) {
        auto be = value::bitcastTo<const char*>(objValue);
        auto end = be + value::readFromMemory<uint32_t>(be);
        // skip document length
        be += 4;
        while (*be != 0) {
            auto sv = bson::fieldNameView(be);

            if (sv == fieldStr) {
                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());
                return {false, tag, val};
            }

            // advance
            be = bson::advance(be, sv.size());
        }
    }
    return {false, value::TypeTags::Nothing, 0};
}

std::pair<value::TypeTags, value::Value> ByteCode::aggSum(value::TypeTags accTag,
                                                          value::Value accValue,
                                                          value::TypeTags fieldTag,
                                                          value::Value fieldValue) {
    if (accTag == value::TypeTags::Nothing) {
        accTag = value::TypeTags::NumberInt64;
        accValue = 0;
    }

    auto [owned, tag, val] = genericAdd(accTag, accValue, fieldTag, fieldValue);

    return {tag, val};
}

bool hasSeparatorAt(size_t idx, std::string_view input, std::string_view separator) {
    if (separator.size() + idx > input.size()) {
        return false;
    }

    return input.compare(idx, separator.size(), separator) == 0;
}
std::tuple<bool, value::TypeTags, value::Value> ByteCode::builtinSplit(uint8_t arity) {
    auto [ownedSeparator, tagSeparator, valSeparator] = getFromStack(0);
    auto [ownedInput, tagInput, valInput] = getFromStack(1);

    if (!value::isString(tagSeparator) || !value::isString(tagInput)) {
        return {false, value::TypeTags::Nothing, 0};
    }

    auto input = value::getStringView(tagInput, valInput);
    auto separator = value::getStringView(tagSeparator, valSeparator);

    auto [tag, val] = value::makeNewArray();
    auto arr = value::getArrayView(val);

    size_t splitStart = 0;
    size_t splitPos;
    while ((splitPos = input.find(separator, splitStart)) != std::string_view::npos) {
        auto [tag, val] = value::makeNewString(input.substr(splitStart, splitPos - splitStart));
        arr->push_back(tag, val);

        splitPos += separator.size();
        splitStart = splitPos;
    }

    // the last string
    {
        auto [tag, val] = value::makeNewString(input.substr(splitStart, input.size() - splitStart));
        arr->push_back(tag, val);
    }

    return {true, tag, val};
}
std::tuple<bool, value::TypeTags, value::Value> ByteCode::builtinDropFields(uint8_t arity) {
    auto [ownedSeparator, tagInObj, valInObj] = getFromStack(0);

    // we operate only on objects
    if (!value::isObject(tagInObj)) {
        return {false, value::TypeTags::Nothing, 0};
    }

    // build the set of fields to drop
    std::set<std::string, std::less<>> restrictFieldsSet;
    for (uint8_t idx = 1; idx < arity; ++idx) {
        auto [owned, tag, val] = getFromStack(idx);

        if (!value::isString(tag)) {
            return {false, value::TypeTags::Nothing, 0};
        }

        restrictFieldsSet.emplace(value::getStringView(tag, val));
    }

    auto [tag, val] = value::makeNewObject();
    auto obj = value::getObjectView(val);

    if (tagInObj == value::TypeTags::bsonObject) {
        auto be = value::bitcastTo<const char*>(valInObj);
        auto end = be + value::readFromMemory<uint32_t>(be);
        // skip document length
        be += 4;
        while (*be != 0) {
            auto sv = bson::fieldNameView(be);

            if (restrictFieldsSet.count(sv) == 0) {
                auto [tag, val] = bson::convertFrom(false, be, end, sv.size());
                obj->push_back(sv, tag, val);
            }

            // advance
            be = bson::advance(be, sv.size());
        }
    } else if (tagInObj == value::TypeTags::Object) {
        auto objRoot = value::getObjectView(valInObj);
        for (size_t idx = 0; idx < objRoot->size(); ++idx) {
            std::string_view sv(objRoot->field(idx));

            if (restrictFieldsSet.count(sv) == 0) {

                auto [tag, val] = objRoot->getAt(idx);
                auto [copyTag, copyVal] = value::copyValue(tag, val);
                obj->push_back(sv, copyTag, copyVal);
            }
        }
    }

    return {true, tag, val};
}
std::tuple<bool, value::TypeTags, value::Value> ByteCode::builtinNewObj(uint8_t arity) {
    std::vector<value::TypeTags> typeTags;
    std::vector<value::Value> values;
    std::vector<std::string> names;

    for (uint8_t idx = 0; idx < arity; idx += 2) {
        {
            auto [owned, tag, val] = getFromStack(idx);

            if (!value::isString(tag)) {
                return {false, value::TypeTags::Nothing, 0};
            }

            names.emplace_back(value::getStringView(tag, val));
        }
        {
            auto [owned, tag, val] = getFromStack(idx + 1);
            typeTags.push_back(tag);
            values.push_back(val);
        }
    }

    auto [tag, val] = value::makeNewObject();
    auto obj = value::getObjectView(val);

    for (size_t idx = 0; idx < typeTags.size(); ++idx) {
        auto [tagCopy, valCopy] = value::copyValue(typeTags[idx], values[idx]);
        obj->push_back(names[idx], tagCopy, valCopy);
    }

    return {true, tag, val};
}
std::tuple<bool, value::TypeTags, value::Value> ByteCode::builtinKeyStringToString(uint8_t arity) {
    auto [owned, tagInKey, valInKey] = getFromStack(0);

    // we operate only on keys
    if (tagInKey != value::TypeTags::ksValue) {
        return {false, value::TypeTags::Nothing, 0};
    }

    auto key = value::getKeyStringView(valInKey);

    auto [tagStr, valStr] = value::makeNewString(key->toString());

    return {true, tagStr, valStr};
}

std::tuple<bool, value::TypeTags, value::Value> ByteCode::builtinNewKeyString(uint8_t arity) {
    auto [_, tagInVersion, valInVersion] = getFromStack(0);

    if (!value::isNumber(tagInVersion) ||
        !(value::numericCast<int64_t>(tagInVersion, valInVersion) == 0 ||
          value::numericCast<int64_t>(tagInVersion, valInVersion) == 1)) {
        return {false, value::TypeTags::Nothing, 0};
    }
    KeyString::Version version =
        static_cast<KeyString::Version>(value::numericCast<int64_t>(tagInVersion, valInVersion));

    auto [__, tagInOrdering, valInOrdering] = getFromStack(1);
    if (!value::isNumber(tagInOrdering)) {
        return {false, value::TypeTags::Nothing, 0};
    }
    auto orderingBits = value::numericCast<int32_t>(tagInOrdering, valInOrdering);
    BSONObjBuilder bb;
    for (size_t i = 0; i < Ordering::kMaxCompoundIndexKeys; ++i) {
        bb.append(""_sd, (orderingBits & (1 << i)) ? 1 : 0);
    }

    KeyString::HeapBuilder kb{version, Ordering::make(bb.done())};

    for (size_t idx = 2; idx < arity - 1u; ++idx) {
        auto [_, tag, val] = getFromStack(idx);
        if (value::isNumber(tag)) {
            auto num = value::numericCast<int64_t>(tag, val);
            kb.appendNumberLong(num);
        } else if (value::isString(tag)) {
            auto str = value::getStringView(tag, val);
            kb.appendString(StringData{str.data(), str.length()});
        } else {
            uasserted(ErrorCodes::InternalError, "unsuppored key string type");
        }
    }

    auto [___, tagInDisrim, valInDiscrim] = getFromStack(arity - 1);
    if (!value::isNumber(tagInDisrim)) {
        return {false, value::TypeTags::Nothing, 0};
    }
    auto discrimNum = value::numericCast<int64_t>(tagInDisrim, valInDiscrim);
    if (discrimNum < 0 || discrimNum > 2) {
        return {false, value::TypeTags::Nothing, 0};
    }

    kb.appendDiscriminator(static_cast<KeyString::Discriminator>(discrimNum));

    return {true, value::TypeTags::ksValue, value::bitcastFrom(new KeyString::Value(kb.release()))};
}


std::tuple<bool, value::TypeTags, value::Value> ByteCode::dispatchBuiltin(Builtin f,
                                                                          uint8_t arity) {
    switch (f) {
        case Builtin::split:
            return builtinSplit(arity);
        case Builtin::dropFields:
            return builtinDropFields(arity);
        case Builtin::newObj:
            return builtinNewObj(arity);
        case Builtin::ksToString:
            return builtinKeyStringToString(arity);
        case Builtin::newKs:
            return builtinNewKeyString(arity);
    }

    invariant(Status(ErrorCodes::InternalError, "builtin function not yet implemented"));
    MONGO_UNREACHABLE;
}
std::tuple<uint8_t, value::TypeTags, value::Value> ByteCode::run(CodeFragment* code) {
    auto pcPointer = code->instrs().data();
    auto pcEnd = pcPointer + code->instrs().size();

    for (;;) {
        if (pcPointer == pcEnd) {
            break;
        } else {
            Instruction i = value::readFromMemory<Instruction>(pcPointer);
            pcPointer += sizeof(i);
            switch (i.tag) {
                case Instruction::pushConstVal: {
                    auto tag = value::readFromMemory<value::TypeTags>(pcPointer);
                    pcPointer += sizeof(tag);
                    auto val = value::readFromMemory<value::Value>(pcPointer);
                    pcPointer += sizeof(val);

                    pushStack(i.owned, tag, val);

                    break;
                }
                case Instruction::pushAccessVal: {
                    auto accessor = value::readFromMemory<value::SlotAccessor*>(pcPointer);
                    pcPointer += sizeof(accessor);

                    auto [tag, val] = accessor->getViewOfValue();
                    pushStack(i.owned, tag, val);

                    break;
                }
                case Instruction::add: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [owned, tag, val] = genericAdd(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::sub: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [owned, tag, val] = genericSub(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::mul: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [owned, tag, val] = genericMul(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::div: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [owned, tag, val] = genericDiv(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::negate: {
                    auto [owned, tag, val] = getFromStack(0);

                    auto [resultOwned, resultTag, resultVal] =
                        genericSub(value::TypeTags::NumberInt32, 0, tag, val);

                    topStack(resultOwned, resultTag, resultVal);

                    if (owned) {
                        value::releaseValue(resultTag, resultVal);
                    }

                    break;
                }
                case Instruction::logicNot: {
                    auto [owned, tag, val] = getFromStack(0);

                    auto [resultOwned, resultTag, resultVal] = genericNot(tag, val);

                    topStack(resultOwned, resultTag, resultVal);

                    if (owned) {
                        value::releaseValue(tag, val);
                    }
                    break;
                }
                case Instruction::less: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] = genericCompare<std::less<>>(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::lessEq: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] =
                        genericCompare<std::less_equal<>>(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::greater: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] =
                        genericCompare<std::greater<>>(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::greaterEq: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] =
                        genericCompare<std::greater_equal<>>(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::eq: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] = genericCompareEq(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::neq: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] = genericCompareNeq(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::cmp3w: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] = compare3way(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::fillEmpty: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    if (lhsTag == value::TypeTags::Nothing) {
                        topStack(rhsOwned, rhsTag, rhsVal);

                        if (lhsOwned) {
                            value::releaseValue(lhsTag, lhsVal);
                        }
                    } else {
                        if (rhsOwned) {
                            value::releaseValue(rhsTag, rhsVal);
                        }
                    }
                    break;
                }
                case Instruction::getField: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [owned, tag, val] = getField(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::sum: {
                    auto [rhsOwned, rhsTag, rhsVal] = getFromStack(0);
                    popStack();
                    auto [lhsOwned, lhsTag, lhsVal] = getFromStack(0);

                    auto [tag, val] = aggSum(lhsTag, lhsVal, rhsTag, rhsVal);

                    topStack(i.owned, tag, val);

                    if (rhsOwned) {
                        value::releaseValue(rhsTag, rhsVal);
                    }
                    if (lhsOwned) {
                        value::releaseValue(lhsTag, lhsVal);
                    }
                    break;
                }
                case Instruction::exists: {
                    auto [owned, tag, val] = getFromStack(0);

                    topStack(i.owned, value::TypeTags::Boolean, tag != value::TypeTags::Nothing);

                    if (owned) {
                        value::releaseValue(tag, val);
                    }
                    break;
                }
                case Instruction::isObject: {
                    auto [owned, tag, val] = getFromStack(0);

                    if (tag != value::TypeTags::Nothing) {
                        topStack(i.owned, value::TypeTags::Boolean, value::isObject(tag));
                    }

                    if (owned) {
                        value::releaseValue(tag, val);
                    }
                    break;
                }
                case Instruction::function: {
                    auto f = value::readFromMemory<Builtin>(pcPointer);
                    pcPointer += sizeof(f);
                    auto arity = value::readFromMemory<uint8_t>(pcPointer);
                    pcPointer += sizeof(arity);

                    auto [owned, tag, val] = dispatchBuiltin(f, arity);

                    for (uint8_t cnt = 0; cnt < arity; ++cnt) {
                        auto [owned, tag, val] = getFromStack(0);
                        popStack();
                        if (owned) {
                            value::releaseValue(tag, val);
                        }
                    }

                    pushStack(owned, tag, val);

                    break;
                }
                case Instruction::jmp: {
                    auto jumpOffset = value::readFromMemory<int>(pcPointer);
                    pcPointer += sizeof(jumpOffset);

                    pcPointer += jumpOffset;
                    break;
                }
                case Instruction::jmpTrue: {
                    auto jumpOffset = value::readFromMemory<int>(pcPointer);
                    pcPointer += sizeof(jumpOffset);

                    auto [owned, tag, val] = getFromStack(0);
                    popStack();

                    if (tag == value::TypeTags::Boolean && val) {
                        pcPointer += jumpOffset;
                    }

                    if (owned) {
                        value::releaseValue(tag, val);
                    }
                    break;
                }
                case Instruction::jmpNothing: {
                    auto jumpOffset = value::readFromMemory<int>(pcPointer);
                    pcPointer += sizeof(jumpOffset);

                    auto [owned, tag, val] = getFromStack(0);
                    if (tag == value::TypeTags::Nothing) {
                        pcPointer += jumpOffset;
                    }
                    break;
                }
                default:
                    invariant(
                        Status(ErrorCodes::InternalError, "vm instruction not yet implemented"));
                    MONGO_UNREACHABLE;
            }
        }
    }

    if (_argStackOwned.size() != 1) {
        invariant(Status(ErrorCodes::InternalError, "error evaluating bytecode"));
        MONGO_UNREACHABLE;
    }

    auto owned = _argStackOwned[0];
    auto tag = _argStackTags[0];
    auto val = _argStackVals[0];

    _argStackOwned.clear();
    _argStackTags.clear();
    _argStackVals.clear();

    return {owned, tag, val};
}

}  // namespace vm
}  // namespace sbe
}  // namespace mongo
