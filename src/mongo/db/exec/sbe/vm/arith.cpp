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

namespace mongo {
namespace sbe {
namespace vm {

using namespace value;

std::tuple<bool, value::TypeTags, value::Value> ByteCode::genericAdd(value::TypeTags lhsTag,
                                                                     value::Value lhsValue,
                                                                     value::TypeTags rhsTag,
                                                                     value::Value rhsValue) {
    if (value::isNumber(lhsTag) && value::isNumber(rhsTag)) {
        switch (getWidestNumericalType(lhsTag, rhsTag)) {
            case value::TypeTags::NumberInt32: {
                auto result =
                    numericCast<int32_t>(lhsTag, lhsValue) + numericCast<int32_t>(rhsTag, rhsValue);
                return {false, value::TypeTags::NumberInt32, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberInt64: {
                auto result =
                    numericCast<int64_t>(lhsTag, lhsValue) + numericCast<int64_t>(rhsTag, rhsValue);
                return {false, value::TypeTags::NumberInt64, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberDouble: {
                auto result =
                    numericCast<double>(lhsTag, lhsValue) + numericCast<double>(rhsTag, rhsValue);
                return {false, value::TypeTags::NumberDouble, value::bitcastFrom(result)};
            }
            case value::TypeTags::NumberDecimal: {
                auto result = numericCast<Decimal128>(lhsTag, lhsValue)
                                  .add(numericCast<Decimal128>(rhsTag, rhsValue));
                auto [tag, val] = value::makeCopyDecimal(result);
                return {true, tag, val};
            }
            default:
                MONGO_UNREACHABLE;
        }
    }

    return {false, value::TypeTags::Nothing, 0};
}

std::pair<value::TypeTags, value::Value> ByteCode::genericCompareEq(value::TypeTags lhsTag,
                                                                    value::Value lhsValue,
                                                                    value::TypeTags rhsTag,
                                                                    value::Value rhsValue) {
    if (value::isNumber(lhsTag) && value::isNumber(rhsTag)) {
        return genericNumericCompare(lhsTag, lhsValue, rhsTag, rhsValue, std::equal_to<>{});
    } else if (value::isString(lhsTag) && value::isString(rhsTag)) {
        auto lhsStr = value::getStringView(lhsTag, lhsValue);
        auto rhsStr = value::getStringView(rhsTag, rhsValue);

        return {value::TypeTags::Boolean, lhsStr.compare(rhsStr) == 0};
    } else if (lhsTag == value::TypeTags::Boolean && rhsTag == value::TypeTags::Boolean) {
        return {value::TypeTags::Boolean, (lhsValue != 0) == (rhsValue != 0)};
    } else if (lhsTag == value::TypeTags::Null && rhsTag == value::TypeTags::Null) {
        // This is where Mongo differs from SQL.
        return {value::TypeTags::Boolean, true};
    } else if (lhsTag == value::TypeTags::ObjectId && rhsTag == value::TypeTags::ObjectId) {
        return {value::TypeTags::Boolean,
                (*value::getObjectIdView(lhsValue)) == (*value::getObjectIdView(rhsValue))};
    } else {
        return {value::TypeTags::Nothing, 0};
    }
}
}  // namespace vm
}  // namespace sbe
}  // namespace mongo
