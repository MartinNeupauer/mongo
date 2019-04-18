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

#include "encryption_schema_tree.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_metadata_tree.h"

namespace mongo {

/**
 * Represents a Pipeline which has been mutated based on an encryption schema tree to contain
 * intent-to-encrypt markings.
 */
class FLEPipeline {
public:
    FLEPipeline(std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
                std::unique_ptr<EncryptionSchemaTreeNode> schema);
    /**
     * Returns the schema of the document flowing *out* of the pipeline.
     */
    const EncryptionSchemaTreeNode& getOutputSchema() const {
        return *_finalSchema.get();
    }

private:
    using MetadataTreeWithFinalSchema =
        std::pair<pipeline_metadata_tree::Stage<clonable_ptr<EncryptionSchemaTreeNode>>,
                  clonable_ptr<EncryptionSchemaTreeNode>>;

    FLEPipeline(MetadataTreeWithFinalSchema&& stageWithOutSchema)
        : _rootStage(std::move(stageWithOutSchema.first)),
          _finalSchema(std::move(stageWithOutSchema.second)){};

    std::unique_ptr<Pipeline, PipelineDeleter> _parsedPipeline;

    // Pointer to the root node of a pipeline metadata tree, where each node in the tree holds a
    // copy of the encryption schema representing the schema of the documents flowing into the
    // corresponding pipeline stage.
    pipeline_metadata_tree::Stage<clonable_ptr<EncryptionSchemaTreeNode>> _rootStage;

    // Schema of the document flowing out of the pipeline, not associated with any Stage.
    clonable_ptr<EncryptionSchemaTreeNode> _finalSchema;
};

}  // namespace mongo
