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

#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/exe_generator.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/bson_scan.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
MONGO_INITIALIZER(RegisterScanImpl)(InitializerContext*) {
    ExeGenerator::_scanImpl = std::mem_fn(&ExeGenerator::walkImpl);

    return Status::OK();
}
/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walkImpl(const Scan& op, const ABT& body) {
    invariant(!_currentStage);
    auto resultInput = generateInputPhase(op.rowsetVar(), body);
    _currentStage = std::move(resultInput.stage);


    auto binder = body.cast<ValueBinder>();
    auto outputSlot = binder->isUsed(op.outputVar())
        ? boost::optional<value::SlotId>{getSlot(binder, op.outputVar())}
        : boost::none;
    if (op.name()) {
        _currentStage = makeS<ScanStage>(*op.name(),
                                         outputSlot,
                                         boost::none,
                                         std::vector<std::string>{},
                                         std::vector<value::SlotId>{},
                                         boost::none,
                                         nullptr);
    } else {
        _currentStage = makeS<BSONScanStage>(op.bsonBegin(),
                                             op.bsonEnd(),
                                             outputSlot,
                                             std::vector<std::string>{},
                                             std::vector<value::SlotId>{});
    }

    auto resultOutput = generateOutputPhase(op.rowsetVar(), body);

    GenResult result;
    result.stage = std::move(resultOutput.stage);
    return result;
}

}  // namespace abt
}  // namespace sbe
}  // namespace mongo
