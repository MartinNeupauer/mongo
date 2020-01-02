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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_stage_builder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/exec/sbe/stages/limit.h"
#include "mongo/db/exec/sbe/stages/loop_join.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
// Returns a non-null pointer to the root of a plan tree, or a non-OK status if the PlanStage tree
// could not be constructed.
std::unique_ptr<sbe::PlanStage> SlotBasedStageBuilder::build(const QuerySolutionNode* root) {
    using namespace std::literals;

    switch (root->getType()) {
        case STAGE_COLLSCAN: {
            auto stage = sbe::makeS<sbe::ScanStage>(
                NamespaceStringOrUUID{_collection->ns().db().toString(), _collection->uuid()},
                "$$RESULT"sv,
                "$$RID"sv,
                std::vector<std::string>{},
                std::vector<std::string>{},
                ""sv);

            if (root->filter) {
                std::string predicateName;
                stage = root->filter->generateStage(std::move(stage), "$$RESULT"sv, predicateName);

                uassert(ErrorCodes::InternalErrorNotSupported,
                        str::stream() << "Cannot build filter",
                        stage && !predicateName.empty());

                stage = sbe::makeS<sbe::FilterStage>(std::move(stage),
                                                     sbe::makeE<sbe::EVariable>(predicateName));
            }

            return stage;
        }
        default: {
            str::stream ss;
            ss << "Can't build exec tree for node ";
            root->appendToString(&ss, 0);
            std::string nodeStr(ss);
            uasserted(ErrorCodes::InternalErrorNotSupported, ss);
        }
    }
}
}  // namespace mongo::stage_builder
