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

#include <array>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "mongo/platform/decimal128.h"
#include "mongo/util/assert_util.h"

namespace mongo {
/**
 * Forward declaration.
 */
namespace KeyString {
class Value;
}
namespace sbe {
namespace value {
using SlotId = int64_t;

/**
 * Type dispatch tags.
 */
enum class TypeTags : uint8_t {
    // The value does not exist, aka Nothing in the Maybe monad.
    Nothing = 0,

    // Numberical data types.
    NumberInt32,
    NumberInt64,
    NumberDouble,
    NumberDecimal,

    // Date data types.
    Date,
    Timestamp,

    Boolean,
    Null,
    StringSmall,
    StringBig,
    Array,
    Object,

    ObjectId,

    // TODO add the rest of mongo types (regex, etc.)

    // Raw bson values.
    bsonObject,
    bsonArray,
    bsonString,
    bsonObjectId,

    // KeyString::Value
    ksValue
};

std::ostream& operator<<(std::ostream& os, const TypeTags tag);

inline constexpr bool isNumber(TypeTags tag) noexcept {
    return tag == TypeTags::NumberInt32 || tag == TypeTags::NumberInt64 ||
        tag == TypeTags::NumberDouble || tag == TypeTags::NumberDecimal;
}

inline constexpr bool isString(TypeTags tag) noexcept {
    return tag == TypeTags::StringSmall || tag == TypeTags::StringBig ||
        tag == TypeTags::bsonString;
}

inline constexpr bool isObject(TypeTags tag) noexcept {
    return tag == TypeTags::Object || tag == TypeTags::bsonObject;
}

inline constexpr bool isArray(TypeTags tag) noexcept {
    return tag == TypeTags::Array || tag == TypeTags::bsonArray;
}

inline constexpr bool isObjectId(TypeTags tag) noexcept {
    return tag == TypeTags::ObjectId || tag == TypeTags::bsonObjectId;
}

/**
 * The runtime value. It is a simple 64 bit integer.
 */
using Value = uint64_t;

/**
 * Forward declarations.
 */
void releaseValue(TypeTags tag, Value val) noexcept;
std::pair<TypeTags, Value> copyValue(TypeTags tag, Value val);

/**
 * Object class.
 */
class Object {
    std::vector<TypeTags> _typeTags;
    std::vector<Value> _values;
    std::vector<std::string> _names;

public:
    Object() = default;
    Object(const Object& other) {
        _values.reserve(other._values.size());
        _typeTags.reserve(other._typeTags.size());
        _names = other._names;
        for (size_t idx = 0; idx < other._values.size(); ++idx) {
            const auto [tag, val] = copyValue(other._typeTags[idx], other._values[idx]);
            _values.push_back(val);
            _typeTags.push_back(tag);
        }
    }
    Object(Object&&) = default;
    ~Object() {
        // TODO in a pathological case sizes are not the same
        for (size_t idx = 0; idx < _typeTags.size(); ++idx) {
            releaseValue(_typeTags[idx], _values[idx]);
        }
    }
    void push_back(std::string_view name, TypeTags tag, Value val) {
        // TODO may leak when out of memory
        if (tag != TypeTags::Nothing) {
            _typeTags.push_back(tag);
            _values.push_back(val);
            _names.emplace_back(std::string(name));
        }
    }

    std::pair<TypeTags, Value> getField(std::string_view field) {
        for (size_t idx = 0; idx < _typeTags.size(); ++idx) {
            if (_names[idx] == field) {
                return {_typeTags[idx], _values[idx]};
            }
        }
        return {TypeTags::Nothing, 0};
    }
    auto size() const noexcept {
        return _values.size();
    }
    auto& field(size_t idx) const {
        return _names[idx];
    }
    std::pair<TypeTags, Value> getAt(std::size_t idx) const {
        if (idx >= _values.size()) {
            return {TypeTags::Nothing, 0};
        }

        return {_typeTags[idx], _values[idx]};
    }
};

/**
 * Array class.
 */
class Array {
    std::vector<TypeTags> _typeTags;
    std::vector<Value> _values;

public:
    Array() = default;
    Array(const Array& other) {
        // TODO this leaks like a seive
        _values.reserve(other._values.size());
        _typeTags.reserve(other._typeTags.size());
        for (size_t idx = 0; idx < other._values.size(); ++idx) {
            const auto [tag, val] = copyValue(other._typeTags[idx], other._values[idx]);
            _values.push_back(val);
            _typeTags.push_back(tag);
        }
    }
    Array(Array&&) = default;
    ~Array() {
        // TODO in a pathological case sizes are not the same
        for (size_t idx = 0; idx < _typeTags.size(); ++idx) {
            releaseValue(_typeTags[idx], _values[idx]);
        }
    }
    void push_back(TypeTags tag, Value val) {
        // TODO may leak when out of memory
        if (tag != TypeTags::Nothing) {
            _typeTags.push_back(tag);
            _values.push_back(val);
        }
    }

    auto size() const noexcept {
        return _values.size();
    }
    std::pair<TypeTags, Value> getAt(std::size_t idx) const {
        if (idx >= _values.size()) {
            return {TypeTags::Nothing, 0};
        }

        return {_typeTags[idx], _values[idx]};
    }
};

constexpr int SmallStringThreshold = 8;
using ObjectIdType = std::array<uint8_t, 12>;
static_assert(sizeof(ObjectIdType) == 12);

inline char* getSmallStringView(Value& val) noexcept {
    return reinterpret_cast<char*>(&val);
}

inline char* getBigStringView(Value val) noexcept {
    return reinterpret_cast<char*>(val);
}

inline char* getRawPointerView(Value val) noexcept {
    return reinterpret_cast<char*>(val);
}

template <typename T>
T readFromMemory(const char* memory) noexcept {
    T val;
    memcpy(&val, memory, sizeof(T));
    return val;
}

template <typename T>
T readFromMemory(const unsigned char* memory) noexcept {
    T val;
    memcpy(&val, memory, sizeof(T));
    return val;
}

template <typename T>
size_t writeToMemory(unsigned char* memory, const T val) noexcept {
    memcpy(memory, &val, sizeof(T));

    return sizeof(T);
}

template <typename T>
Value bitcastFrom(const T in) noexcept {
    static_assert(sizeof(Value) >= sizeof(T));

    // casting from pointer to integer value is OK
    if constexpr (std::is_pointer_v<T>) {
        return reinterpret_cast<Value>(in);
    }
    Value val{0};
    memcpy(&val, &in, sizeof(T));
    return val;
}
template <typename T>
T bitcastTo(const Value in) noexcept {
    static_assert(sizeof(Value) >= sizeof(T));

    // casting from integer value to pointer is OK
    if constexpr (std::is_pointer_v<T>) {
        return reinterpret_cast<T>(in);
    } else {
        T val;
        memcpy(&val, &in, sizeof(T));
        return val;
    }
}

inline std::string_view getStringView(TypeTags tag, Value& val) noexcept {
    if (tag == TypeTags::StringSmall) {
        return std::string_view(getSmallStringView(val));
    } else if (tag == TypeTags::StringBig) {
        return std::string_view(getBigStringView(val));
    } else if (tag == TypeTags::bsonString) {
        auto bsonstr = getRawPointerView(val);
        return std::string_view(bsonstr + 4, readFromMemory<uint32_t>(bsonstr) - 1);
    }

    invariant(Status(ErrorCodes::InternalError, "getStringView called with non string tag."));
    MONGO_UNREACHABLE;
}

inline std::pair<TypeTags, Value> makeNewString(std::string_view input) {
    size_t len = input.size();
    if (len < SmallStringThreshold - 1) {
        Value smallString;
        auto stringAlias =
            getSmallStringView(smallString);  // THIS IS OK - aliasing to char* !!! :)
        memcpy(stringAlias, input.data(), len);
        stringAlias[len] = 0;
        return {TypeTags::StringSmall, smallString};
    } else {
        auto str = new char[len + 1];
        memcpy(str, input.data(), len);
        str[len] = 0;
        return {TypeTags::StringBig, reinterpret_cast<Value>(str)};
    }
}

inline std::pair<TypeTags, Value> makeNewArray() {
    auto a = new Array;
    return {TypeTags::Array, reinterpret_cast<Value>(a)};
}

inline std::pair<TypeTags, Value> makeCopyArray(const Array& inA) {
    auto a = new Array(inA);
    return {TypeTags::Array, reinterpret_cast<Value>(a)};
}

inline Array* getArrayView(Value val) noexcept {
    return reinterpret_cast<Array*>(val);
}

inline std::pair<TypeTags, Value> makeNewObject() {
    auto o = new Object;
    return {TypeTags::Object, reinterpret_cast<Value>(o)};
}

inline std::pair<TypeTags, Value> makeCopyObject(const Object& inO) {
    auto o = new Object(inO);
    return {TypeTags::Object, reinterpret_cast<Value>(o)};
}

inline Object* getObjectView(Value val) noexcept {
    return reinterpret_cast<Object*>(val);
}

inline std::pair<TypeTags, Value> makeNewObjectId() {
    auto o = new ObjectIdType;
    return {TypeTags::ObjectId, reinterpret_cast<Value>(o)};
}

inline std::pair<TypeTags, Value> makeCopyObjectId(const ObjectIdType& inO) {
    auto o = new ObjectIdType(inO);
    return {TypeTags::ObjectId, reinterpret_cast<Value>(o)};
}

inline ObjectIdType* getObjectIdView(Value val) noexcept {
    return reinterpret_cast<ObjectIdType*>(val);
}

inline Decimal128* getDecimalView(Value val) noexcept {
    return reinterpret_cast<Decimal128*>(val);
}

inline std::pair<TypeTags, Value> makeCopyDecimal(const Decimal128& inD) {
    auto o = new Decimal128(inD);
    return {TypeTags::NumberDecimal, reinterpret_cast<Value>(o)};
}

inline KeyString::Value* getKeyStringView(Value val) noexcept {
    return reinterpret_cast<KeyString::Value*>(val);
}

std::pair<TypeTags, Value> makeCopyKeyString(const KeyString::Value& inKey);

void releaseValue(TypeTags tag, Value val) noexcept;

inline std::pair<TypeTags, Value> copyValue(TypeTags tag, Value val) {
    switch (tag) {
        case TypeTags::NumberDecimal:
            return makeCopyDecimal(*getDecimalView(val));
        case TypeTags::Array:
            return makeCopyArray(*getArrayView(val));
        case TypeTags::Object:
            return makeCopyObject(*getObjectView(val));
        case TypeTags::StringBig: {
            auto src = getBigStringView(val);
            auto len = strlen(src);
            auto dst = new char[len + 1];
            memcpy(dst, src, len);
            dst[len] = 0;
            return {TypeTags::StringBig, bitcastFrom(dst)};
        }
        case TypeTags::bsonString: {
            auto bsonstr = getRawPointerView(val);
            auto src = bsonstr + 4;
            auto size = readFromMemory<uint32_t>(bsonstr);
            return makeNewString(std::string_view(src, size - 1));
        }
        case TypeTags::ObjectId: {
            return makeCopyObjectId(*getObjectIdView(val));
        }
        case TypeTags::bsonObject: {
            auto bson = bitcastTo<uint8_t*>(val);
            auto size = readFromMemory<uint32_t>(bson);
            auto dst = new uint8_t[size];
            memcpy(dst, bson, size);
            return {TypeTags::bsonObject, bitcastFrom(dst)};
        }
        case TypeTags::bsonObjectId: {
            auto bson = bitcastTo<uint8_t*>(val);
            auto size = sizeof(ObjectIdType);
            auto dst = new uint8_t[size];
            memcpy(dst, bson, size);
            return {TypeTags::bsonObjectId, bitcastFrom(dst)};
        }
        case TypeTags::bsonArray: {
            auto bson = bitcastTo<uint8_t*>(val);
            auto size = readFromMemory<uint32_t>(bson);
            auto dst = new uint8_t[size];
            memcpy(dst, bson, size);
            return {TypeTags::bsonArray, bitcastFrom(dst)};
        }
        case TypeTags::ksValue:
            return makeCopyKeyString(*getKeyStringView(val));
        default:
            break;
    }

    return {tag, val};
}

// implicit conversions of numerical types
template <typename T>
inline T numericCast(TypeTags tag, Value val) noexcept {
    switch (tag) {
        case TypeTags::NumberInt32:
            if constexpr (std::is_same_v<T, Decimal128>) {
                return Decimal128(bitcastTo<int32_t>(val));
            } else {
                return bitcastTo<int32_t>(val);
            }
        case TypeTags::NumberInt64:
            if constexpr (std::is_same_v<T, Decimal128>) {
                return Decimal128(bitcastTo<int64_t>(val));
            } else {
                return bitcastTo<int64_t>(val);
            }
        case TypeTags::NumberDouble:
            if constexpr (std::is_same_v<T, Decimal128>) {
                return Decimal128(bitcastTo<double>(val));
            } else {
                return bitcastTo<double>(val);
            }
        case TypeTags::NumberDecimal:
            if constexpr (std::is_same_v<T, Decimal128>) {
                return *getDecimalView(val);
            }
            MONGO_UNREACHABLE;
        default:
            MONGO_UNREACHABLE;
    }
}

inline TypeTags getWidestNumericalType(TypeTags lhsTag, TypeTags rhsTag) noexcept {
    if (lhsTag == TypeTags::NumberDecimal || rhsTag == TypeTags::NumberDecimal) {
        return TypeTags::NumberDecimal;
    } else if (lhsTag == TypeTags::NumberDouble || rhsTag == TypeTags::NumberDouble) {
        return TypeTags::NumberDouble;
    } else if (lhsTag == TypeTags::NumberInt64 || rhsTag == TypeTags::NumberInt64) {
        return TypeTags::NumberInt64;
    } else if (lhsTag == TypeTags::NumberInt32 || rhsTag == TypeTags::NumberInt32) {
        return TypeTags::NumberInt32;
    } else {
        MONGO_UNREACHABLE;
    }
}

std::size_t hashValue(TypeTags tag, Value val) noexcept;
std::pair<TypeTags, Value> eqValue(TypeTags lhsTag,
                                   Value lhsValue,
                                   TypeTags rhsTag,
                                   Value rhsValue);
std::pair<TypeTags, Value> lessValue(TypeTags lhsTag,
                                     Value lhsValue,
                                     TypeTags rhsTag,
                                     Value rhsValue);

class SlotAccessor {
public:
    virtual ~SlotAccessor() = 0;
    virtual std::pair<TypeTags, Value> getViewOfValue() const = 0;
    virtual std::pair<TypeTags, Value> copyOrMoveValue() = 0;
};
inline SlotAccessor::~SlotAccessor() {}

class ViewOfValueAccessor final : public SlotAccessor {
    TypeTags _tag{TypeTags::Nothing};
    Value _val{0};

public:
    // Return non-owning view of the value.
    std::pair<TypeTags, Value> getViewOfValue() const override {
        return {_tag, _val};
    }

    // Return a copy.
    std::pair<TypeTags, Value> copyOrMoveValue() override {
        return copyValue(_tag, _val);
    }

    void reset() {
        reset(TypeTags::Nothing, 0);
    }

    void reset(TypeTags tag, Value val) {
        _tag = tag;
        _val = val;
    }
};

class OwnedValueAccessor final : public SlotAccessor {
    TypeTags _tag{TypeTags::Nothing};
    Value _val;
    bool _owned{false};

    void release() {
        if (_owned) {
            releaseValue(_tag, _val);
            _owned = false;
        }
    }

public:
    ~OwnedValueAccessor() {
        // release any value here
        release();
    }

    // Return non-owning view of the value
    std::pair<TypeTags, Value> getViewOfValue() const override {
        return {_tag, _val};
    }

    std::pair<TypeTags, Value> copyOrMoveValue() override {
        if (_owned) {
            _owned = false;
            return {_tag, _val};
        } else {
            return copyValue(_tag, _val);
        }
    }

    void reset() {
        reset(TypeTags::Nothing, 0);
    }

    void reset(TypeTags tag, Value val) {
        reset(true, tag, val);
    }

    void reset(bool owned, TypeTags tag, Value val) {
        // release any value here
        release();

        _tag = tag;
        _val = val;
        _owned = owned;
    }
};

class ArrayAccessor final : public SlotAccessor {
    TypeTags _tagArray{TypeTags::Nothing};
    Value _valArray{0};

    // Array
    Array* _array{nullptr};
    size_t _index{0};

    // bsonArray
    const char* _arrayCurrent{nullptr};
    const char* _arrayEnd{nullptr};

public:
    void reset(TypeTags tag, Value val) {
        _tagArray = tag;
        _valArray = val;
        _array = nullptr;
        _index = 0;

        if (tag == TypeTags::Array) {
            _array = getArrayView(val);
        } else if (tag == TypeTags::bsonArray) {
            auto bson = bitcastTo<const char*>(val);
            _arrayCurrent = bson + 4;
            _arrayEnd = bson + value::readFromMemory<uint32_t>(bson);
        } else {
            MONGO_UNREACHABLE;
        }
    }

    // Return non-owning view of the value
    std::pair<TypeTags, Value> getViewOfValue() const override;
    std::pair<TypeTags, Value> copyOrMoveValue() override {
        // we can never move out values from array
        auto [tag, val] = getViewOfValue();
        return copyValue(tag, val);
    }

    bool atEnd() const {
        if (_array) {
            return _index == _array->size();
        } else {
            return *_arrayCurrent == 0;
        }
    }

    bool advance();
};

struct MaterializedRow {
    std::vector<OwnedValueAccessor> _fields;

    void makeOwned() {
        for (auto& f : _fields) {
            auto [tag, val] = f.getViewOfValue();
            auto [copyTag, copyVal] = copyValue(tag, val);
            f.reset(copyTag, copyVal);
        }
    }
    bool operator==(const MaterializedRow& rhs) const {
        for (size_t idx = 0; idx < _fields.size(); ++idx) {
            auto [lhsTag, lhsVal] = _fields[idx].getViewOfValue();
            auto [rhsTag, rhsVal] = rhs._fields[idx].getViewOfValue();
            auto [tag, val] = eqValue(lhsTag, lhsVal, rhsTag, rhsVal);

            if (!((tag == TypeTags::Boolean) && (val != 0))) {
                return false;
            }
        }

        return true;
    }

    bool operator<(const MaterializedRow& rhs) const {
        for (size_t idx = 0; idx < _fields.size(); ++idx) {
            auto [lhsTag, lhsVal] = _fields[idx].getViewOfValue();
            auto [rhsTag, rhsVal] = rhs._fields[idx].getViewOfValue();
            {
                auto [tag, val] = lessValue(lhsTag, lhsVal, rhsTag, rhsVal);

                if ((tag == TypeTags::Boolean) && (val != 0)) {
                    return true;
                }
            }
            {
                auto [tag, val] = eqValue(lhsTag, lhsVal, rhsTag, rhsVal);

                if ((tag == TypeTags::Boolean) && (val == 0)) {
                    return false;
                }
            }
        }

        return false;
    }
};

struct MaterializedRowHasher {
    std::size_t operator()(const MaterializedRow& k) const {
        size_t res = 17;
        for (auto& f : k._fields) {
            auto [tag, val] = f.getViewOfValue();
            res = res * 31 + hashValue(tag, val);
        }
        return res;
    }
};

template <typename T>
class MaterializedRowKeyAccessor final : public SlotAccessor {
    T& _it;
    size_t _slot;

public:
    MaterializedRowKeyAccessor(T& it, size_t slot) : _it(it), _slot(slot) {}

    std::pair<TypeTags, Value> getViewOfValue() const override {
        return _it->first._fields[_slot].getViewOfValue();
    }
    std::pair<TypeTags, Value> copyOrMoveValue() override {
        // we can never move out values from keys
        auto [tag, val] = getViewOfValue();
        return copyValue(tag, val);
    }
};

template <typename T>
class MaterializedRowValueAccessor final : public SlotAccessor {
    T& _it;
    size_t _slot;

public:
    MaterializedRowValueAccessor(T& it, size_t slot) : _it(it), _slot(slot) {}

    std::pair<TypeTags, Value> getViewOfValue() const override {
        return _it->second._fields[_slot].getViewOfValue();
    }
    std::pair<TypeTags, Value> copyOrMoveValue() override {
        return _it->second._fields[_slot].copyOrMoveValue();
    }

    void reset(bool owned, TypeTags tag, Value val) {
        _it->second._fields[_slot].reset(owned, tag, val);
    }
};

}  // namespace value
}  // namespace sbe
}  // namespace mongo
