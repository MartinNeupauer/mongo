/**
 *    Copyright (C) 2017 MongoDB Inc.
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
 *    linked combinations including the prograxm with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/bson/inline_decls.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/platform/random.h"
#include "mongo/s/chunk_manager.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/timer.h"

namespace mongo {
namespace {

ChunkRange getRangeForChunk(int i, int nChunks) {
    ASSERT_GTE(i, 0);
    ASSERT_GT(nChunks, 0);
    ASSERT_LT(i, nChunks);
    if (i == 0) {
        return {BSON("_id" << MINKEY), BSON("_id" << 0)};
    }
    if (i + 1 == nChunks) {
        return {BSON("_id" << (i - 1) * 100), BSON("_id" << MAXKEY)};
    }
    return {BSON("_id" << (i - 1) * 100), BSON("_id" << i * 100)};
}

template <typename ShardSelectorFn>
auto makeChunkManagerWithShardSelector(Timer& t,
                                       int nShards,
                                       int nChunks,
                                       ShardSelectorFn selectShard) {
    const auto collEpoch = OID::gen();
    const auto collName = NamespaceString("test.foo");
    const auto shardKeyPattern = KeyPattern(BSON("_id" << 1));

    std::vector<ChunkType> chunks;
    chunks.reserve(nChunks);
    for (int i = 0; i < nChunks; ++i) {
        chunks.emplace_back(collName,
                            getRangeForChunk(i, nChunks),
                            ChunkVersion{i + 1, 0, collEpoch},
                            selectShard(i, nShards, nChunks));
    }
    t.reset();
    auto chunkManager =
        ChunkManager::makeNew(collName, {}, shardKeyPattern, nullptr, true, collEpoch, chunks);
    return stdx::make_unique<CollectionMetadata>(std::move(chunkManager), ShardId("shard0"));
}

ShardId pessimalShardSelector(int i, int nShards, int nChunks) {
    return ShardId(str::stream() << "shard" << (i % nShards));
}

ShardId optimalShardSelector(int i, int nShards, int nChunks) {
    ASSERT_LTE(nShards, nChunks);
    const auto shardNum = (int64_t(i) * nShards / nChunks) % nShards;
    return ShardId(str::stream() << "shard" << shardNum);
}

NOINLINE_DECL auto makeChunkManagerWithPessimalBalancedDistribution(Timer& t,
                                                                    int nShards,
                                                                    int nChunks) {
    return makeChunkManagerWithShardSelector(t, nShards, nChunks, pessimalShardSelector);
}

NOINLINE_DECL auto makeChunkManagerWithOptimalBalancedDistribution(Timer& t,
                                                                   int nShards,
                                                                   int nChunks) {
    return makeChunkManagerWithShardSelector(t, nShards, nChunks, optimalShardSelector);
}

NOINLINE_DECL auto runIncrementalUpdate(CollectionMetadata const& cm,
                                        std::vector<ChunkType> const& newChunks) {
    return stdx::make_unique<CollectionMetadata>(cm.getChunkManager()->makeUpdated(newChunks),
                                                 ShardId("shard0"));
}

class ChunkManagerBenchmark : public unittest::Test {};

TEST_F(ChunkManagerBenchmark, IncrementalRefresh) {
    const int nShards = 2;
    const int nChunks = 50000;
    Timer t;
    auto cm = makeChunkManagerWithPessimalBalancedDistribution(t, nShards, nChunks);
    const auto initialBuildTime = t.elapsed();
    auto postMoveVersion = cm->getChunkManager()->getVersion();
    const auto collName = NamespaceString(cm->getChunkManager()->getns());
    std::vector<ChunkType> newChunks;
    postMoveVersion.incMajor();
    newChunks.emplace_back(
        collName, getRangeForChunk(1, nChunks), postMoveVersion, ShardId("shard0"));
    postMoveVersion.incMajor();
    newChunks.emplace_back(
        collName, getRangeForChunk(3, nChunks), postMoveVersion, ShardId("shard1"));
    t.reset();
    auto newCM = runIncrementalUpdate(*cm, newChunks);
    auto incrementalUpdateTime = t.elapsed();
    log() << "Full build of " << nChunks << " chunks took " << initialBuildTime;
    log() << "Incremental update took " << incrementalUpdateTime;
    log() << "Final chunks == " << newCM->getChunkManager()->numChunks();
    //log() << "Final range map size " << newCM->getNumChunks();
}

template <typename CollectionMetadataBuilderFn>
void bmChunkManagerFinds(const int nShards,
                         const int nChunks,
                         CollectionMetadataBuilderFn makeCollectionMetadata) {
    constexpr int nFinds = 200000;
    static_assert(nFinds % 2 == 0, "");
    Timer t;
    auto cm = makeCollectionMetadata(t, nShards, nChunks);
    const auto initialBuildTime = t.elapsed();
    PseudoRandom rand(12345);
    std::vector<BSONObj> keys;
    for (int i = 0; i < nFinds; ++i) {
        keys.push_back(BSON("_id" << rand.nextInt64(nChunks * 100)));
    }
    std::vector<std::pair<BSONObj, BSONObj>> ranges;
    for (size_t i = 0; i < keys.size(); i += 2) {
        auto k1 = keys[i];
        auto k2 = keys[i + 1];
        if (SimpleBSONObjComparator::kInstance.evaluate(k1 == k2)) {
            continue;
        }
        if (SimpleBSONObjComparator::kInstance.evaluate(k1 > k2)) {
            std::swap(k1, k2);
        }
        ranges.push_back(std::make_pair(k1, k2));
    }
    t.reset();
    for (const auto& k : keys) {
        cm->getChunkManager()->findIntersectingChunkWithSimpleCollation(k);
    }
    const auto findChunkTime = t.elapsed();
    t.reset();
    for (const auto& range : ranges) {
        std::set<ShardId> shardIds;
        cm->getChunkManager()->getShardIdsForRange(range.first, range.second, &shardIds);
    }
    const auto getShardIdsTime = t.elapsed();
    t.reset();
    size_t nOwned = 0;
    for (const auto& k : keys) {
        if (cm->keyBelongsToMe(k)) {
            ++nOwned;
        }
    }
    const auto keyBelongsToMeTime = t.elapsed();
    t.reset();
    size_t nOverlapped = 0;
    for (const auto& range : ranges) {
        if (cm->rangeOverlapsChunk(ChunkRange(range.first, range.second))) {
            ++nOverlapped;
        }
    }
    const auto rangeOverlapsChunkTime = t.elapsed();
    log() << "Full build of " << nChunks << " chunks took " << initialBuildTime;
    log() << "Calling findIntersectingChunk " << keys.size() << " times took " << findChunkTime;
    log() << "Calling getShardIdsForRange " << ranges.size() << " times took " << getShardIdsTime;
    log() << "Calling keyBelongsToMe " << nFinds << " times found we own " << nOwned
          << " keys and took " << duration_cast<Nanoseconds>(keyBelongsToMeTime) / nFinds
          << " per call";
    log() << "Calling rangeOverlapsChunk " << ranges.size() << " times found we overlap "
          << nOverlapped << " ranges and took "
          << duration_cast<Nanoseconds>(rangeOverlapsChunkTime) /
            static_cast<int64_t>(ranges.size())
          << " per call";
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks2ShardsPessimal) {
    bmChunkManagerFinds(2, 50000, makeChunkManagerWithPessimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks10ShardsPessimal) {
    bmChunkManagerFinds(10, 50000, makeChunkManagerWithPessimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks100ShardsPessimal) {
    bmChunkManagerFinds(100, 50000, makeChunkManagerWithPessimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks1000ShardsPessimal) {
    bmChunkManagerFinds(1000, 50000, makeChunkManagerWithPessimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith2Chunks2ShardsPessimal) {
    bmChunkManagerFinds(2, 2, makeChunkManagerWithPessimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks2ShardsOptimal) {
    bmChunkManagerFinds(2, 50000, makeChunkManagerWithOptimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks10ShardsOptimal) {
    bmChunkManagerFinds(10, 50000, makeChunkManagerWithOptimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks100ShardsOptimal) {
    bmChunkManagerFinds(100, 50000, makeChunkManagerWithOptimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith50kChunks1000ShardsOptimal) {
    bmChunkManagerFinds(1000, 50000, makeChunkManagerWithOptimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FindsWith2Chunks2ShardsOptimal) {
    bmChunkManagerFinds(2, 2, makeChunkManagerWithOptimalBalancedDistribution);
}

TEST_F(ChunkManagerBenchmark, FullLoad) {
    const int nShards = 2;
    const int nChunks = 4;
    const auto collEpoch = OID::gen();
    const auto collName = NamespaceString("test.foo");
    const auto shardKeyPattern = KeyPattern(BSON("_id" << 1));

    std::vector<ChunkType> chunks;
    chunks.reserve(nChunks);
    for (int i = 0; i < nChunks; ++i) {
        chunks.emplace_back(collName,
                            getRangeForChunk(i, nChunks),
                            ChunkVersion{0, i + 1, collEpoch},
                            ShardId(str::stream() << "shard" << (i % nShards)));
    }
    auto updatedChunkManager =
        ChunkManager::makeNew(collName, {}, shardKeyPattern, nullptr, true, collEpoch, chunks);
    ASSERT_EQ(4, updatedChunkManager->numChunks());
}

}  // namespace
}  // namespace mongo
