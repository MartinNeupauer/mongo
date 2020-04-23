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

#include "mongo/db/exec/sbe/values/bson.h"

namespace mongo {
namespace sbe {
namespace bson {
// clang-format off
static uint8_t advanceTable[] = {
	0xff, // End
	8,    // double
	0xff, // string
	0xfe, // document
	0xfe, // document
	0x80, // binary ??? +1 ?
	0,    // Undefined(value) - Deprecated
	12,   // ObjectId
	1,    // Boolean
	8,    // UTC datetime
	0,    // Null value
	0x80, // Regular expression
	0x80, // DBPointer
	0x80, // JavaScript code
	0x80, // Symbol
	0x80, // JavaScript code w/ scope ????
	4,    // 32-bit integer
	8,    // Timestamp
	8,    // 64-bit integer
	16    // 128-bit decimal floating point

};
// clang-format on

enum class bsonType : uint8_t {
    Double = 1,
    String,
    Object,
    Array,
    Binary,
    Undefined,
    ObjectId,
    Boolean,
    Date,
    Null,
    RegExp,
    DBPointer,
    JavaScript,
    Symbol,
    JavaScriptWS,
    Integer32,
    Timestamp,
    Integer64,
    Decimal128
};

static_assert((int)bsonType::Decimal128 == 19);

const char* advance(const char* be, size_t fieldNameSize) {
    auto type = static_cast<unsigned char>(*be);

    be += 1 /*type*/ + fieldNameSize + 1 /*zero at the end of fieldname*/;
    if (type < sizeof(advanceTable)) {
        auto advOffset = advanceTable[type];
        if (advOffset < 128) {
            be += advOffset;
        } else {
            be += value::readFromMemory<uint32_t>(be);
            if (advOffset == 0xff) {
                be += 4;
            } else if (advOffset == 0xfe) {
            } else {
                if (static_cast<bsonType>(type) == bsonType::Binary) {
                    be += 5;
                } else {
                    invariant(Status(ErrorCodes::InternalError, "unsupported bson element"));
                }
            }
        }
    } else {
        invariant(Status(ErrorCodes::InternalError, "unsupported bson element"));
    }

    return be;
}

std::pair<value::TypeTags, value::Value> convertFrom(bool view,
                                                     const char* be,
                                                     const char* end,
                                                     size_t fieldNameSize) {
    bsonType type = static_cast<bsonType>(*be);
    // advance be
    be += 1 + fieldNameSize + 1;

    switch (type) {
        case bsonType::Double: {
            auto dbl = value::readFromMemory<double>(be);
            return {value::TypeTags::NumberDouble, value::bitcastFrom(dbl)};
        }
        case bsonType::Decimal128: {
            if (view) {
                return {value::TypeTags::NumberDecimal, value::bitcastFrom(be)};
            }
            auto dec = value::readFromMemory<Decimal128>(be);
            return value::makeCopyDecimal(dec);
        }
        case bsonType::String: {
            if (view) {
                return {value::TypeTags::bsonString, value::bitcastFrom(be)};
            }
            // len includes trailing zero
            int32_t len = value::readFromMemory<int32_t>(be);
            be += sizeof(len);
            if (len < value::SmallStringThreshold) {
                value::Value smallString;
                // copy fast 8 bytes if we have space
                if (be + 8 < end) {
                    memcpy(&smallString, be, 8);
                } else {
                    memcpy(&smallString, be, len);
                }
                return {value::TypeTags::StringSmall, smallString};
            } else {
                auto str = new char[len];
                memcpy(str, be, len);
                return {value::TypeTags::StringBig, value::bitcastFrom(str)};
            }
        }
        case bsonType::Object: {
            if (view) {
                return {value::TypeTags::bsonObject, value::bitcastFrom(be)};
            }
            // skip document length
            be += 4;
            auto [tag, val] = value::makeNewObject();
            auto obj = value::getObjectView(val);

            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                auto [tag, val] = convertFrom(false, be, end, sv.size());
                obj->push_back(sv, tag, val);

                // advance
                be = advance(be, sv.size());
            }
            return {tag, val};
        }
        case bsonType::Array: {
            if (view) {
                return {value::TypeTags::bsonArray, value::bitcastFrom(be)};
            }
            // skip array length
            be += 4;
            auto [tag, val] = value::makeNewArray();
            auto arr = value::getArrayView(val);

            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                auto [tag, val] = convertFrom(false, be, end, sv.size());
                arr->push_back(tag, val);

                // advance
                be = advance(be, sv.size());
            }
            return {tag, val};
        }
        case bsonType::ObjectId: {
            if (view) {
                return {value::TypeTags::bsonObjectId, value::bitcastFrom(be)};
            }
            auto [tag, val] = value::makeNewObjectId();
            memcpy(value::getObjectIdView(val), be, sizeof(value::ObjectIdType));
            return {tag, val};
        }
        case bsonType::Boolean:
            return {value::TypeTags::Boolean, *(be)};
        case bsonType::Date: {
            auto integer = value::readFromMemory<int64_t>(be);
            return {value::TypeTags::Date, value::bitcastFrom(integer)};
        }
        case bsonType::Null:
            return {value::TypeTags::Null, 0};
        case bsonType::Integer32: {
            auto integer = value::readFromMemory<int32_t>(be);
            return {value::TypeTags::NumberInt32, value::bitcastFrom(integer)};
        }
        case bsonType::Timestamp: {
            auto val = value::readFromMemory<uint64_t>(be);
            return {value::TypeTags::Timestamp, value::bitcastFrom(val)};
        }
        case bsonType::Integer64: {
            auto val = value::readFromMemory<int64_t>(be);
            return {value::TypeTags::NumberInt64, value::bitcastFrom(val)};
        }
        default:
            return {value::TypeTags::Nothing, 0};
    }
}
void convertToBsonObj(BSONArrayBuilder& builder, value::Array* arr) {
    for (size_t idx = 0; idx < arr->size(); ++idx) {
        auto [tag, val] = arr->getAt(idx);

        switch (tag) {
            case value::TypeTags::Nothing:
                break;
            case value::TypeTags::NumberInt32:
                builder.append(value::bitcastTo<int32_t>(val));
                break;
            case value::TypeTags::NumberInt64:
                builder.append(value::bitcastTo<int64_t>(val));
                break;
            case value::TypeTags::NumberDouble:
                builder.append(value::bitcastTo<double>(val));
                break;
            case value::TypeTags::NumberDecimal:
                builder.append(*value::getDecimalView(val));
                break;
            case value::TypeTags::Date:
                builder.append(Date_t::fromMillisSinceEpoch(value::bitcastTo<int64_t>(val)));
                break;
            case value::TypeTags::Timestamp:
                builder.append(Timestamp(value::bitcastTo<uint64_t>(val)));
                break;
            case value::TypeTags::Boolean:
                builder.append(val != 0);
                break;
            case value::TypeTags::Null:
                builder.appendNull();
                break;
            case value::TypeTags::StringSmall:
            case value::TypeTags::StringBig:
            case value::TypeTags::bsonString: {
                auto sv = value::getStringView(tag, val);
                builder.append(StringData{sv.data(), sv.size()});
                break;
            }
            case value::TypeTags::Array: {
                BSONArrayBuilder subarrBuilder(builder.subarrayStart());
                convertToBsonObj(subarrBuilder, value::getArrayView(val));
                subarrBuilder.doneFast();
                break;
            }
            case value::TypeTags::Object: {
                BSONObjBuilder subobjBuilder(builder.subobjStart());
                convertToBsonObj(subobjBuilder, value::getObjectView(val));
                subobjBuilder.doneFast();
                break;
            }
            case value::TypeTags::ObjectId:
                builder.append(OID::from(value::getObjectIdView(val)->data()));
                break;
            case value::TypeTags::bsonObject:
                builder.append(BSONObj{value::bitcastTo<const char*>(val)});
                break;
            case value::TypeTags::bsonArray:
                builder.append(BSONArray{BSONObj{value::bitcastTo<const char*>(val)}});
                break;
            case value::TypeTags::bsonObjectId:
                builder.append(OID::from(value::bitcastTo<const char*>(val)));
                break;
            default:
                MONGO_UNREACHABLE;
        }
    }
}
void convertToBsonObj(BSONObjBuilder& builder, value::Object* obj) {
    for (size_t idx = 0; idx < obj->size(); ++idx) {
        auto [tag, val] = obj->getAt(idx);
        const auto& name = obj->field(idx);

        switch (tag) {
            case value::TypeTags::Nothing:
                break;
            case value::TypeTags::NumberInt32:
                builder.append(name, value::bitcastTo<int32_t>(val));
                break;
            case value::TypeTags::NumberInt64:
                builder.append(name, value::bitcastTo<int64_t>(val));
                break;
            case value::TypeTags::NumberDouble:
                builder.append(name, value::bitcastTo<double>(val));
                break;
            case value::TypeTags::NumberDecimal:
                builder.append(name, *value::getDecimalView(val));
                break;
            case value::TypeTags::Date:
                builder.append(name, Date_t::fromMillisSinceEpoch(value::bitcastTo<int64_t>(val)));
                break;
            case value::TypeTags::Timestamp:
                builder.append(name, Timestamp(value::bitcastTo<uint64_t>(val)));
                break;
            case value::TypeTags::Boolean:
                builder.append(name, val != 0);
                break;
            case value::TypeTags::Null:
                builder.appendNull(name);
                break;
            case value::TypeTags::StringSmall:
            case value::TypeTags::StringBig:
            case value::TypeTags::bsonString: {
                auto sv = value::getStringView(tag, val);
                builder.append(name, StringData{sv.data(), sv.size()});
                break;
            }
            case value::TypeTags::Array: {
                BSONArrayBuilder subarrBuilder(builder.subarrayStart(name));
                convertToBsonObj(subarrBuilder, value::getArrayView(val));
                subarrBuilder.doneFast();
                break;
            }
            case value::TypeTags::Object: {
                BSONObjBuilder subobjBuilder(builder.subobjStart(name));
                convertToBsonObj(subobjBuilder, value::getObjectView(val));
                subobjBuilder.doneFast();
                break;
            }
            case value::TypeTags::ObjectId:
                builder.append(name, OID::from(value::getObjectIdView(val)->data()));
                break;
            case value::TypeTags::bsonObject:
                builder.appendObject(name, value::bitcastTo<const char*>(val));
                break;
            case value::TypeTags::bsonArray:
                builder.appendArray(name, BSONObj{value::bitcastTo<const char*>(val)});
                break;
            case value::TypeTags::bsonObjectId:
                builder.append(name, OID::from(value::bitcastTo<const char*>(val)));
                break;
            default:
                MONGO_UNREACHABLE;
        }
    }
}
}  // namespace bson
}  // namespace sbe
}  // namespace mongo
