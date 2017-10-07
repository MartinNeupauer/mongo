/**
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/record_id.h"

#include "mongo/db/codegen/operator/entry_functions.h"

#include <memory>
#include <vector>

namespace machine { class Jitter; }

namespace mongo {

/**
 * This stage runs the generated native code
 */
class CodeGenStage final : public PlanStage {
public:
    CodeGenStage(OperationContext* opCtx, WorkingSet* workingSet);

    ~CodeGenStage();

    bool isEOF() final;
    StageState doWork(WorkingSetID* out) final;


    StageType stageType() const final {
        return STAGE_EOF;
    }

    std::unique_ptr<PlanStageStats> getStats();

    const SpecificStats* getSpecificStats() const final;

    bool translate(Pipeline* pipeline);

    static const char* kStageType;
private:

    // The Jitter that translates IR to the native code
    // TODO: Temporary placement
    std::unique_ptr<machine::Jitter> _jitter;

    // The buffer that holds any internal state needed by the compiled query (if any)
    std::vector<char> _stateBuffer;

    // State
    rohan::RuntimeState _state;

    // Native function pointer to the Open call
    rohan::NativeOpenFunction _openFunction {nullptr};

    // Native function pointer to the GetNext call
    rohan::NativeGetNextFunction _getNextFunction {nullptr};

    // Is this the first call to doWork?
    bool _first {true};

    // WorkingSet is not owned by us.
    WorkingSet* _workingSet;
};

}  // namespace mongo
