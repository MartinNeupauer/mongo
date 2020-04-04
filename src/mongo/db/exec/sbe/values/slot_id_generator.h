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
 * An interface for FrameId generators, which provides a single method to generate unique FrameId's.
 * The uniqueness guarantees are the same as for SlotIdGenerator.
 */
class FrameIdGenerator {
public:
    virtual ~FrameIdGenerator() = default;

    /**
     * Generates a new FrameId which is always different from all previously generated.
     */
    virtual FrameId generate() = 0;
};

/**
 * A reusable id generator suitable for use with integer ids that generates each new id by adding an
 * increment to the previously generated id. This generator is not thread safe; calls to
 * generateByIncrementing must be serialized.
 */
template <class T>
class IncrementingIdGenerator {
protected:
    /**
     * Constructs a new generator using 'startingId' as the first generated id and 'incrementStep'
     * as the value to add to generate subsequent ids. Note that 'incrementStep' may be negative but
     * must not be zero.
     */
    IncrementingIdGenerator(T startingId, T incrementStep)
        : _currentId(startingId), _incrementStep(incrementStep) {}

    T generateByIncrementing() {
        _currentId += _incrementStep;
        return _currentId;
    }

private:
    T _currentId;
    T _incrementStep;
};

template <class T>
class IdGenerator : IncrementingIdGenerator<T> {
public:
    IdGenerator(T startingId = 0, T incrementStep = 1)
        : IncrementingIdGenerator<T>(startingId, incrementStep) {}

    T generate() {
        return this->generateByIncrementing();
    }
};


class IncrementingSlotIdGenerator : IncrementingIdGenerator<value::SlotId>, public SlotIdGenerator {
public:
    IncrementingSlotIdGenerator(int64_t startingSlotId, int64_t incrementStep)
        : IncrementingIdGenerator<value::SlotId>(startingSlotId, incrementStep) {}

    value::SlotId generate() final {
        return generateByIncrementing();
    }
};

class IncrementingFrameIdGenerator : IncrementingIdGenerator<FrameId>, public FrameIdGenerator {
public:
    IncrementingFrameIdGenerator(int64_t startingFrameId, int64_t incrementStep)
        : IncrementingIdGenerator<FrameId>(startingFrameId, incrementStep) {}

    FrameId generate() final {
        return generateByIncrementing();
    }
};

/**
 * Constructs a new SlotIdGenerator using a default implemenation algorithm for generating SlotId's.
 */
inline std::unique_ptr<SlotIdGenerator> makeDefaultSlotIdGenerator() {
    return std::make_unique<IncrementingSlotIdGenerator>(0, 1);
}

/**
 * Constructs a new FrameIdGenerator using a default implemenation algorithm for generating
 * FrameId's.
 */
inline std::unique_ptr<FrameIdGenerator> makeDefaultFrameIdGenerator() {
    return std::make_unique<IncrementingFrameIdGenerator>(0, 1);
}
}  // namespace mongo::sbe::value
