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

#include "mongo/base/init.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/test_commands_enabled.h"
#include "mongo/db/cursor_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/sbe/parser/parser.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/query/cursor_request.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/find_common.h"

using namespace std::literals;

namespace mongo {
class SBECommand final : public BasicCommand {
public:
    SBECommand() : BasicCommand("sbe") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext* context) const override {
        return AllowedOnSecondary::kOptIn;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {
        CommandHelpers::handleMarkKillOnClientDisconnect(opCtx);
        long long batchSize;
        uassertStatusOK(CursorRequest::parseCommandCursorOptions(
            cmdObj, std::numeric_limits<long long>::max(), &batchSize));

        sbe::Parser parser;
        auto root = parser.parse(opCtx, dbname, cmdObj["sbe"].String());
        auto [resultSlot, recordIdSlot] = parser.getTopLevelSlots();

        std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec;
        BSONArrayBuilder firstBatch;

        NamespaceString nss{dbname, "$cmd.sbe"_sd};
        invariant(nss.isCollectionlessCursorNamespace());

        exec = uassertStatusOK(PlanExecutor::make(
            opCtx, nullptr, {std::move(root), {resultSlot, recordIdSlot}}, nss, nullptr));

        for (long long objCount = 0; objCount < batchSize; objCount++) {
            BSONObj next;
            PlanExecutor::ExecState state = exec->getNext(&next, nullptr);
            if (state == PlanExecutor::IS_EOF) {
                break;
            }
            invariant(state == PlanExecutor::ADVANCED);

            // If we can't fit this result inside the current batch, then we stash it for later.
            if (!FindCommon::haveSpaceForNext(next, objCount, firstBatch.len())) {
                exec->enqueue(next);
                break;
            }

            firstBatch.append(next);
        }
        if (exec->isEOF()) {
            appendCursorResponseObject(0LL, nss.ns(), firstBatch.arr(), &result);
            return true;
        }

        exec->saveState();
        exec->detachFromOperationContext();
        const auto pinnedCursor = CursorManager::get(opCtx)->registerCursor(
            opCtx,
            {
                std::move(exec),
                nss,
                AuthorizationSession::get(opCtx->getClient())->getAuthenticatedUserNames(),
                opCtx->getWriteConcern(),
                repl::ReadConcernArgs::get(opCtx),
                cmdObj,
                {Privilege(ResourcePattern::forExactNamespace(nss), ActionType::find)},
                false  // needsMerge always 'false' for sbe.
            });

        appendCursorResponseObject(
            pinnedCursor.getCursor()->cursorid(), nss.ns(), firstBatch.arr(), &result);

        return true;
    }
};

MONGO_REGISTER_TEST_COMMAND(SBECommand);
}  // namespace mongo
