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

#include "mongo/db/exec/sbe/values/value.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/storage/key_string.h"

namespace mongo {
namespace sbe {
namespace value {

std::pair<TypeTags, Value> makeCopyKeyString(const KeyString::Value& inKey) {
    auto k = new KeyString::Value(inKey);
    return {TypeTags::ksValue, reinterpret_cast<Value>(k)};
}

void releaseValue(TypeTags tag, Value val) noexcept {
    switch (tag) {
        case TypeTags::NumberDecimal:
            delete getDecimalView(val);
            break;
        case TypeTags::Array:
            delete getArrayView(val);
            break;
        case TypeTags::Object:
            delete getObjectView(val);
            break;
        case TypeTags::StringBig:
            delete[] getBigStringView(val);
            break;
        case TypeTags::ObjectId:
            delete getObjectIdView(val);
            break;
        case TypeTags::bsonObject:
        case TypeTags::bsonArray:
        case TypeTags::bsonObjectId:
            delete[] bitcastTo<uint8_t*>(val);
            break;
        case TypeTags::ksValue:
            delete getKeyStringView(val);
            break;
        default:
            break;
    }
}

std::ostream& operator<<(std::ostream& os, const TypeTags tag) {
    switch (tag) {
        case TypeTags::Nothing:
            os << "Nothing";
            break;
        case TypeTags::NumberInt32:
            os << "NumberInt32";
            break;
        case TypeTags::NumberInt64:
            os << "NumberInt64";
            break;
        case TypeTags::NumberDouble:
            os << "NumberDouble";
            break;
        case TypeTags::NumberDecimal:
            os << "NumberDecimal";
            break;
        case TypeTags::Date:
            os << "Date";
            break;
        case TypeTags::Timestamp:
            os << "Timestamp";
            break;
        case TypeTags::Boolean:
            os << "Boolean";
            break;
        case TypeTags::Null:
            os << "Null";
            break;
        case TypeTags::StringSmall:
            os << "StringSmall";
            break;
        case TypeTags::StringBig:
            os << "StringBig";
            break;
        case TypeTags::Array:
            os << "Array";
            break;
        case TypeTags::Object:
            os << "Object";
            break;
        case TypeTags::ObjectId:
            os << "ObjectId";
            break;
        case TypeTags::bsonObject:
            os << "bsonObject";
            break;
        case TypeTags::bsonArray:
            os << "bsonArray";
            break;
        case TypeTags::bsonString:
            os << "bsonString";
            break;
        case TypeTags::bsonObjectId:
            os << "bsonObjectId";
            break;
        default:
            os << "unknown tag";
            break;
    }
    return os;
}

void printValue(std::ostream& os, TypeTags tag, Value val) {
    switch (tag) {
        case value::TypeTags::NumberInt32:
            os << bitcastTo<int32_t>(val);
            break;
        case value::TypeTags::NumberInt64:
            os << bitcastTo<int64_t>(val);
            break;
        case value::TypeTags::NumberDouble:
            os << bitcastTo<double>(val);
            break;
        case value::TypeTags::NumberDecimal:
            os << getDecimalView(val)->toString();
            break;
        case value::TypeTags::Boolean:
            os << ((val) ? "true" : "false");
            break;
        case value::TypeTags::Null:
            os << "null";
            break;
        case value::TypeTags::StringSmall:
            os << '"' << getSmallStringView(val) << '"';
            break;
        case value::TypeTags::StringBig:
            os << '"' << getBigStringView(val) << '"';
            break;
        case value::TypeTags::Array: {
            auto arr = getArrayView(val);
            os << '[';
            for (size_t idx = 0; idx < arr->size(); ++idx) {
                if (idx != 0) {
                    os << ", ";
                }
                auto [tag, val] = arr->getAt(idx);
                printValue(os, tag, val);
            }
            os << ']';
            break;
        }
        case value::TypeTags::Object: {
            auto obj = getObjectView(val);
            os << '{';
            for (size_t idx = 0; idx < obj->size(); ++idx) {
                if (idx != 0) {
                    os << ", ";
                }
                os << '"' << obj->field(idx) << '"';
                os << " : ";
                auto [tag, val] = obj->getAt(idx);
                printValue(os, tag, val);
            }
            os << '}';
            break;
        }
        case value::TypeTags::ObjectId: {
            auto objId = getObjectIdView(val);
            os << "ObjectId(\"" << OID::from(objId->data()).toString() << "\")";
            break;
        }
        case value::TypeTags::Nothing:
            os << "---===*** NOTHING ***===---";
            break;
        case value::TypeTags::bsonArray: {
            auto be = value::bitcastTo<const char*>(val);
            auto end = be + value::readFromMemory<uint32_t>(be);
            bool first = true;
            // skip document length
            be += 4;
            os << '[';
            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                if (!first) {
                    os << ", ";
                }
                first = false;

                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());
                printValue(os, tag, val);

                // advance
                be = bson::advance(be, sv.size());
            }
            os << ']';
            break;
        }
        case value::TypeTags::bsonObject: {
            auto be = value::bitcastTo<const char*>(val);
            auto end = be + value::readFromMemory<uint32_t>(be);
            bool first = true;
            // skip document length
            be += 4;
            os << '{';
            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                if (!first) {
                    os << ", ";
                }
                first = false;

                os << '"' << sv << '"';
                os << " : ";
                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());
                printValue(os, tag, val);

                // advance
                be = bson::advance(be, sv.size());
            }
            os << '}';
            break;
        }
        case value::TypeTags::bsonString:
            os << '"' << getStringView(value::TypeTags::bsonString, val) << '"';
            break;
        case value::TypeTags::bsonObjectId:
            os << "---===*** bsonObjectId ***===---";
            break;
        case value::TypeTags::ksValue: {
            auto ks = getKeyStringView(val);
            os << "KS(" << ks->toString() << ")";
            break;
        }
        default:
            MONGO_UNREACHABLE;
    }
}

std::size_t hashValue(TypeTags tag, Value val) noexcept {
    switch (tag) {
        case TypeTags::NumberInt32:
            return bitcastTo<int32_t>(val);
        case TypeTags::NumberInt64:
            return bitcastTo<int64_t>(val);
        case TypeTags::NumberDouble:
            return bitcastTo<double>(val);
        case TypeTags::NumberDecimal:
            return getDecimalView(val)->toLong();
        case TypeTags::Date:
            return bitcastTo<int64_t>(val);
        case TypeTags::Timestamp:
            return bitcastTo<uint64_t>(val);
        case TypeTags::Boolean:
            return val != 0;
        case TypeTags::Null:
            return 0;
        case TypeTags::StringSmall:
        case TypeTags::StringBig:
        case TypeTags::bsonString: {
            auto sv = getStringView(tag, val);
            return std::hash<std::string_view>()(sv);
        }
        case TypeTags::ObjectId: {
            auto id = getObjectIdView(val);
            return std::hash<uint64_t>()(readFromMemory<uint64_t>(id->data())) ^
                std::hash<uint32_t>()(readFromMemory<uint32_t>(id->data() + 8));
        }
        case TypeTags::ksValue: {
            return getKeyStringView(val)->hash();
        }
        default:
            break;
    }

    return 0;
}


// comparison used by a hash table
std::pair<TypeTags, Value> eqValue(TypeTags lhsTag,
                                   Value lhsValue,
                                   TypeTags rhsTag,
                                   Value rhsValue) {
    if (isNumber(lhsTag) && isNumber(rhsTag)) {
        switch (getWidestNumericalType(lhsTag, rhsTag)) {
            case TypeTags::NumberInt32: {
                auto result = numericCast<int32_t>(lhsTag, lhsValue) ==
                    numericCast<int32_t>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberInt64: {
                auto result = numericCast<int64_t>(lhsTag, lhsValue) ==
                    numericCast<int64_t>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberDouble: {
                auto result =
                    numericCast<double>(lhsTag, lhsValue) == numericCast<double>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberDecimal: {
                auto result = numericCast<Decimal128>(lhsTag, lhsValue)
                                  .isEqual(numericCast<Decimal128>(rhsTag, rhsValue));
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            default:
                MONGO_UNREACHABLE;
        }
    } else if (isString(lhsTag) && isString(rhsTag)) {
        auto lhsStr = getStringView(lhsTag, lhsValue);
        auto rhsStr = getStringView(rhsTag, rhsValue);

        return {TypeTags::Boolean, lhsStr.compare(rhsStr) == 0};
    } else if (lhsTag == TypeTags::Date && rhsTag == TypeTags::Date) {
        return {TypeTags::Boolean, bitcastTo<int64_t>(lhsValue) == bitcastTo<int64_t>(rhsValue)};
    } else if (lhsTag == TypeTags::Timestamp && rhsTag == TypeTags::Timestamp) {
        return {TypeTags::Boolean, bitcastTo<uint64_t>(lhsValue) == bitcastTo<uint64_t>(rhsValue)};
    } else if (lhsTag == TypeTags::Boolean && rhsTag == TypeTags::Boolean) {
        return {TypeTags::Boolean, (lhsValue != 0) == (rhsValue != 0)};
    } else if (lhsTag == TypeTags::Null && rhsTag == TypeTags::Null) {
        // This is where Mongo differs from SQL.
        return {TypeTags::Boolean, true};
    } else if (lhsTag == TypeTags::ObjectId && rhsTag == TypeTags::ObjectId) {
        return {TypeTags::Boolean, (*getObjectIdView(lhsValue)) == (*getObjectIdView(rhsValue))};
    } else if (lhsTag == TypeTags::ksValue && rhsTag == TypeTags::ksValue) {
        return {TypeTags::Boolean,
                getKeyStringView(lhsValue)->compare(*getKeyStringView(lhsValue)) == 0};
    } else if (lhsTag == TypeTags::Nothing && rhsTag == TypeTags::Nothing) {
        // special case for Nothing in a hash table (group) comparison
        return {TypeTags::Boolean, 1};
    } else {
        return {TypeTags::Nothing, 0};
    }
}

// comparison used by a sort operator
std::pair<TypeTags, Value> lessValue(TypeTags lhsTag,
                                     Value lhsValue,
                                     TypeTags rhsTag,
                                     Value rhsValue) {
    if (isNumber(lhsTag) && isNumber(rhsTag)) {
        switch (getWidestNumericalType(lhsTag, rhsTag)) {
            case TypeTags::NumberInt32: {
                auto result =
                    numericCast<int32_t>(lhsTag, lhsValue) < numericCast<int32_t>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberInt64: {
                auto result =
                    numericCast<int64_t>(lhsTag, lhsValue) < numericCast<int64_t>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberDouble: {
                auto result =
                    numericCast<double>(lhsTag, lhsValue) < numericCast<double>(rhsTag, rhsValue);
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            case TypeTags::NumberDecimal: {
                auto result = numericCast<Decimal128>(lhsTag, lhsValue)
                                  .isLess(numericCast<Decimal128>(rhsTag, rhsValue));
                return {TypeTags::Boolean, bitcastFrom(result)};
            }
            default:
                MONGO_UNREACHABLE;
        }
    } else if (isString(lhsTag) && isString(rhsTag)) {
        auto lhsStr = getStringView(lhsTag, lhsValue);
        auto rhsStr = getStringView(rhsTag, rhsValue);

        return {TypeTags::Boolean, lhsStr.compare(rhsStr) < 0};
    } else if (lhsTag == TypeTags::Date && rhsTag == TypeTags::Date) {
        return {TypeTags::Boolean, bitcastTo<int64_t>(lhsValue) < bitcastTo<int64_t>(rhsValue)};
    } else if (lhsTag == TypeTags::Timestamp && rhsTag == TypeTags::Timestamp) {
        return {TypeTags::Boolean, bitcastTo<uint64_t>(lhsValue) < bitcastTo<uint64_t>(rhsValue)};
    } else if (lhsTag == TypeTags::Boolean && rhsTag == TypeTags::Boolean) {
        return {TypeTags::Boolean, (lhsValue != 0) < (rhsValue != 0)};
    } else if (lhsTag == TypeTags::ObjectId && rhsTag == TypeTags::ObjectId) {
        return {TypeTags::Boolean, (*getObjectIdView(lhsValue)) < (*getObjectIdView(rhsValue))};
    } else if (lhsTag == TypeTags::ksValue && rhsTag == TypeTags::ksValue) {
        return {TypeTags::Boolean,
                getKeyStringView(lhsValue)->compare(*getKeyStringView(lhsValue)) < 0};
    } else if (lhsTag == TypeTags::Nothing && rhsTag == TypeTags::Nothing) {
        // special case for Nothing
        return {TypeTags::Boolean, 0};
    } else if (lhsTag == TypeTags::Nothing) {
        // special case for Nothing
        return {TypeTags::Boolean, 1};
    } else if (rhsTag == TypeTags::Nothing) {
        // special case for Nothing
        return {TypeTags::Boolean, 0};
    } else {
        return {TypeTags::Nothing, 0};
    }
}

std::pair<TypeTags, Value> ArrayAccessor::getViewOfValue() const {
    if (_array) {
        return _array->getAt(_index);
    } else {
        auto sv = bson::fieldNameView(_arrayCurrent);
        return bson::convertFrom(true, _arrayCurrent, _arrayEnd, sv.size());
    }
}

bool ArrayAccessor::advance() {
    if (_array) {
        if (_index < _array->size()) {
            ++_index;
        }

        return _index < _array->size();
    } else {
        if (*_arrayCurrent != 0) {
            auto sv = bson::fieldNameView(_arrayCurrent);
            _arrayCurrent = bson::advance(_arrayCurrent, sv.size());
        }

        return *_arrayCurrent != 0;
    }
}

}  // namespace value
}  // namespace sbe
}  // namespace mongo