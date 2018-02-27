/**
 *    Copyright (C) 2012-2015 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/type_chunk.h"

#include <cstring>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

const NamespaceString ChunkType::ConfigNS("config.chunks");
const std::string ChunkType::ShardNSPrefix = "config.cache.chunks.";

const BSONField<std::string> ChunkType::name("_id");
const BSONField<BSONObj> ChunkType::minShardID("_id");
const BSONField<std::string> ChunkType::ns("ns");
const BSONField<BSONObj> ChunkType::min("min");
const BSONField<BSONObj> ChunkType::max("max");
const BSONField<std::string> ChunkType::shard("shard");
const BSONField<bool> ChunkType::jumbo("jumbo");
const BSONField<Date_t> ChunkType::lastmod("lastmod");
const BSONField<OID> ChunkType::epoch("lastmodEpoch");
const BSONField<BSONArray> ChunkType::history("history");

namespace {

const char kMinKey[] = "min";
const char kMaxKey[] = "max";

/**
 * Extracts an Object value from 'obj's field 'fieldName'. Sets the result to 'bsonElement'.
 */
Status extractObject(const BSONObj& obj, const std::string& fieldName, BSONElement* bsonElement) {
    Status elementStatus = bsonExtractTypedField(obj, fieldName, Object, bsonElement);
    if (!elementStatus.isOK()) {
        return elementStatus.withContext(str::stream() << "The field '" << fieldName
                                                       << "' cannot be parsed");
    }

    if (bsonElement->Obj().isEmpty()) {
        return {ErrorCodes::BadValue,
                str::stream() << "The field '" << fieldName << "' cannot be empty"};
    }

    auto validForStorageStatus = bsonElement->Obj().storageValidEmbedded();
    if (!validForStorageStatus.isOK()) {
        return validForStorageStatus;
    }

    return Status::OK();
}

}  // namespace

ChunkRange::ChunkRange(BSONObj minKey, BSONObj maxKey)
    : _minKey(std::move(minKey)), _maxKey(std::move(maxKey)) {
    dassert(SimpleBSONObjComparator::kInstance.evaluate(_minKey < _maxKey));
}

StatusWith<ChunkRange> ChunkRange::fromBSON(const BSONObj& obj) {
    BSONElement minKey;
    {
        Status minKeyStatus = extractObject(obj, kMinKey, &minKey);
        if (!minKeyStatus.isOK()) {
            return minKeyStatus;
        }
    }

    BSONElement maxKey;
    {
        Status maxKeyStatus = extractObject(obj, kMaxKey, &maxKey);
        if (!maxKeyStatus.isOK()) {
            return maxKeyStatus;
        }
    }

    if (SimpleBSONObjComparator::kInstance.evaluate(minKey.Obj() >= maxKey.Obj())) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "min: " << minKey.Obj() << " should be less than max: "
                              << maxKey.Obj()};
    }

    return ChunkRange(minKey.Obj().getOwned(), maxKey.Obj().getOwned());
}

bool ChunkRange::containsKey(const BSONObj& key) const {
    return _minKey.woCompare(key) <= 0 && key.woCompare(_maxKey) < 0;
}

void ChunkRange::append(BSONObjBuilder* builder) const {
    builder->append(kMinKey, _minKey);
    builder->append(kMaxKey, _maxKey);
}

std::string ChunkRange::toString() const {
    return str::stream() << "[" << _minKey << ", " << _maxKey << ")";
}

bool ChunkRange::operator==(const ChunkRange& other) const {
    return _minKey.woCompare(other._minKey) == 0 && _maxKey.woCompare(other._maxKey) == 0;
}

bool ChunkRange::operator!=(const ChunkRange& other) const {
    return !(*this == other);
}

bool ChunkRange::covers(ChunkRange const& other) const {
    auto le = [](auto const& a, auto const& b) { return a.woCompare(b) <= 0; };
    return le(_minKey, other._minKey) && le(other._maxKey, _maxKey);
}

boost::optional<ChunkRange> ChunkRange::overlapWith(ChunkRange const& other) const {
    auto le = [](auto const& a, auto const& b) { return a.woCompare(b) <= 0; };
    if (le(other._maxKey, _minKey) || le(_maxKey, other._minKey)) {
        return boost::none;
    }
    return ChunkRange(le(_minKey, other._minKey) ? other._minKey : _minKey,
                      le(_maxKey, other._maxKey) ? _maxKey : other._maxKey);
}

ChunkRange ChunkRange::unionWith(ChunkRange const& other) const {
    auto le = [](auto const& a, auto const& b) { return a.woCompare(b) <= 0; };
    return ChunkRange(le(_minKey, other._minKey) ? _minKey : other._minKey,
                      le(_maxKey, other._maxKey) ? other._maxKey : _maxKey);
}

// ChunkType

ChunkType::ChunkType() = default;

ChunkType::ChunkType(NamespaceString nss, ChunkRange range, ChunkVersion version, ShardId shardId)
    : _version(version) {
    ChunkTypeBase::setMin(range.getMin());
    ChunkTypeBase::setMax(range.getMax());
    ChunkTypeBase::setNss(std::move(nss));
    ChunkTypeBase::setShard(std::move(shardId));
    ChunkTypeBase::setLastmod(Timestamp(_version->toLong()));
    ChunkTypeBase::setLastmodEpoch(_version->epoch());
}

StatusWith<ChunkType> ChunkType::fromConfigBSON(const BSONObj& source) {
    try {
        ChunkType chunk(parse(IDLParserErrorContext("chunk type"), source));

        chunk.set_id(boost::none);

        {
            auto versionStatus = ChunkVersion::parseFromBSONForChunk(source);
            if (!versionStatus.isOK()) {
                return versionStatus.getStatus();
            }
            chunk._version = std::move(versionStatus.getValue());
        }

        return chunk;
    } catch (const DBException& e) {
       return Status(ErrorCodes::BadValue, e.what());
    }
}

BSONObj ChunkType::toConfigBSON() const {
    BSONObjBuilder builder;

    serialize(&builder);

    if (ChunkTypeBase::getNss().is_initialized() && ChunkTypeBase::getMin().is_initialized())
        builder.append(name.name(), getName());

    return builder.obj();
}

StatusWith<ChunkType> ChunkType::fromShardBSON(const BSONObj& source, const OID& epoch) {
    ChunkType chunk;

    {
        BSONElement minKey;
        Status minKeyStatus = extractObject(source, minShardID.name(), &minKey);
        if (!minKeyStatus.isOK()) {
            return minKeyStatus;
        }

        BSONElement maxKey;
        Status maxKeyStatus = extractObject(source, max.name(), &maxKey);
        if (!maxKeyStatus.isOK()) {
            return maxKeyStatus;
        }

        if (SimpleBSONObjComparator::kInstance.evaluate(minKey.Obj() >= maxKey.Obj())) {
            return {ErrorCodes::FailedToParse,
                    str::stream() << "min: " << minKey.Obj() << " should be less than max: "
                                  << maxKey.Obj()};
        }

        chunk.setMin(minKey.Obj().getOwned());
        chunk.setMax(maxKey.Obj().getOwned());
    }

    {
        std::string chunkShard;
        Status status = bsonExtractStringField(source, shard.name(), &chunkShard);
        if (!status.isOK())
            return status;
        chunk.setShard(chunkShard);
    }

    {
        auto statusWithChunkVersion =
            ChunkVersion::parseFromBSONWithFieldAndSetEpoch(source, lastmod.name(), epoch);
        if (!statusWithChunkVersion.isOK()) {
            return statusWithChunkVersion.getStatus();
        }
        chunk._version = std::move(statusWithChunkVersion.getValue());
    }

    return chunk;
}

BSONObj ChunkType::toShardBSON() const {
    BSONObjBuilder builder;
    invariant(ChunkTypeBase::getMin().is_initialized());
    invariant(ChunkTypeBase::getMax().is_initialized());
    invariant(ChunkTypeBase::getShard().is_initialized());
    invariant(_version);
    builder.append(minShardID.name(), getMin());
    builder.append(max.name(), getMax());
    builder.append(shard.name(), getShard().toString());
    builder.appendTimestamp(lastmod.name(), _version->toLong());
    return builder.obj();
}

std::string ChunkType::getName() const {
    invariant(ChunkTypeBase::getNss().is_initialized());
    invariant(ChunkTypeBase::getMin().is_initialized());
    return genID(getNS(), getMin());
}

void ChunkType::setNS(const NamespaceString& nss) {
    invariant(nss.isValid());
    ChunkTypeBase::setNss(nss);
}

void ChunkType::setMin(const BSONObj& min) {
    invariant(!min.isEmpty());
    ChunkTypeBase::setMin(min);
}

void ChunkType::setMax(const BSONObj& max) {
    invariant(!max.isEmpty());
    ChunkTypeBase::setMax(max);
}

void ChunkType::setVersion(const ChunkVersion& version) {
    invariant(version.isSet());
    _version = version;
    ChunkTypeBase::setLastmod(Timestamp(_version->toLong());
    ChunkTypeBase::setLastmodEpoch(_version->epoch());
}

void ChunkType::setShard(const ShardId& shard) {
    invariant(shard.isValid());
    ChunkTypeBase::setShard(shard);
}

std::string ChunkType::genID(const NamespaceString& nss, const BSONObj& o) {
    StringBuilder buf;
    buf << nss.ns() << "-";

    BSONObjIterator i(o);
    while (i.more()) {
        BSONElement e = i.next();
        buf << e.fieldName() << "_" << e.toString(false, true);
    }

    return buf.str();
}

Status ChunkType::validate() const {
    if (!ChunkTypeBase::getMin().is_initialized() || getMin().isEmpty()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing " << min.name() << " field");
    }

    if (!ChunkTypeBase::getMax().is_initialized() || getMax().isEmpty()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing " << max.name() << " field");
    }

    if (!_version.is_initialized() || !_version->isSet()) {
        return Status(ErrorCodes::NoSuchKey, str::stream() << "missing version field");
    }

    if (!ChunkTypeBase::getShard().is_initialized() || !getShard().isValid()) {
        return Status(ErrorCodes::NoSuchKey,
                      str::stream() << "missing " << shard.name() << " field");
    }

    // 'min' and 'max' must share the same fields.
    if (getMin().nFields() != getMax().nFields()) {
        return {ErrorCodes::BadValue,
                str::stream() << "min and max don't have the same number of keys: " << getMin()
                              << ", "
                              << getMax()};
    }

    BSONObjIterator minIt(getMin());
    BSONObjIterator maxIt(getMax());
    while (minIt.more() && maxIt.more()) {
        BSONElement minElem = minIt.next();
        BSONElement maxElem = maxIt.next();
        if (strcmp(minElem.fieldName(), maxElem.fieldName())) {
            return {ErrorCodes::BadValue,
                    str::stream() << "min and max don't have matching keys: " << getMin() << ", "
                                  << getMax()};
        }
    }

    // 'max' should be greater than 'min'.
    if (getMin().woCompare(getMax()) >= 0) {
        return {ErrorCodes::BadValue,
                str::stream() << "max is not greater than min: " << getMin() << ", " << getMax()};
    }

    return Status::OK();
}

std::string ChunkType::toString() const {
    // toConfigBSON will include all the set fields, whereas toShardBSON includes only a subset and
    // requires them to be set.
    return toConfigBSON().toString();
}

}  // namespace mongo
