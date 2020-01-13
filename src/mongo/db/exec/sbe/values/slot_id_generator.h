/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

namespace mongo::sbe::value {
/*
 * System reserved slot ids.
 */
enum SystemSlots : int64_t { kResultSlot = -1, kRecordIdSlot = -2 };

/**
 * An interface for SlotId generators which provides a single method to generate unique SlotId's.
 * The uniqueness of the generated SlotId's is restricted to the life time of a specific SlotId
 * generator. That is, two different generators may produce non-unique SlotId's, but the the same
 * generator, unless destroyed, must never return the same SlotId and multiple calls to generate()
 * must return different SlotId's.
 */
class SlotIdGenerator {
public:
    virtual ~SlotIdGenerator() = default;

    /**
     * Generates a new SlotId which is always different from all previously generated.
     */
    virtual value::SlotId generate() = 0;
};

/**
 * A SlotId generator which generates SlotId's by incrementing a previous one by the
 * specified value. This class is not thread-safe and must not be accessed concurrently.
 */
class IncrementingSlotIdGenerator : public SlotIdGenerator {
public:
    /**
     * Constructs a new generator using 'startingSlotId' as the first generated SlotId,
     * and 'incrementStep' value to be used as an increment for following SlotId's,
     * which can be negative.
     */
    IncrementingSlotIdGenerator(int64_t startingSlotId, int64_t incrementStep)
        : _currentSlotId{startingSlotId}, _incrementStep{incrementStep} {}

    value::SlotId generate() final {
        _currentSlotId += _incrementStep;
        return _currentSlotId;
    }

private:
    int64_t _currentSlotId;
    int64_t _incrementStep;
};

/**
 * Constructs a new SlotIdGenerator using a default implemenation algorithm for generating SlotId's.
 */
inline std::unique_ptr<SlotIdGenerator> makeDefaultSlotIdGenerator() {
    return std::make_unique<IncrementingSlotIdGenerator>(0, 1);
}
}  // namespace mongo::sbe::value
