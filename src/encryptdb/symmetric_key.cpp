/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "symmetric_key.h"

#include <cstring>

#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/secure_zero_memory.h"

namespace mongo {
SymmetricKey::SymmetricKey(const uint8_t* key,
                           size_t keySize,
                           uint32_t algorithm,
                           std::string keyId,
                           uint32_t initializationCount)
    : _algorithm(algorithm),
      _keySize(keySize),
      _keyId(std::move(keyId)),
      _initializationCount(initializationCount) {
    if (_keySize < crypto::minKeySize || _keySize > crypto::maxKeySize) {
        error() << "Attempt to construct symmetric key of invalid size: " << _keySize;
        return;
    }
    _key.reset(new uint8_t[keySize]);
    memcpy(_key.get(), key, _keySize);
}

SymmetricKey::SymmetricKey(std::unique_ptr<uint8_t[]> key,
                           size_t keySize,
                           uint32_t algorithm,
                           std::string keyId)
    : _algorithm(algorithm),
      _keySize(keySize),
      _key(std::move(key)),
      _keyId(std::move(keyId)),
      _initializationCount(1) {}

SymmetricKey::SymmetricKey(SymmetricKey&& sk)
    : _algorithm(sk._algorithm),
      _keySize(sk._keySize),
      _key(std::move(sk._key)),
      _keyId(std::move(sk._keyId)),
      _initializationCount(sk._initializationCount) {}

SymmetricKey& SymmetricKey::operator=(SymmetricKey&& sk) {
    _algorithm = sk._algorithm;
    _keySize = sk._keySize;
    _key = std::move(sk._key);
    _keyId = std::move(sk._keyId);
    _initializationCount = sk._initializationCount;

    return *this;
}

SymmetricKey::~SymmetricKey() {
    if (_key) {
        secureZeroMemory(_key.get(), _keySize);
    }
}
}  // namespace mongo
