/**
 *  Copyright (C) 2015 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "symmetric_key.h"

#include <cstring>

#include "mongo/util/log.h"
#include "mongo/util/secure_zero_memory.h"
#include "mongo/util/str.h"
#include "symmetric_crypto.h"

namespace mongo {

std::string SymmetricKeyId::_initStrRep() const {
    return str::stream() << _name << " (" << _id << ")";
}

const std::string& SymmetricKeyId::toString() const {
    if (!_strRep.empty()) {
        return _strRep;
    } else {
        return _name;
    }
}

SymmetricKey::SymmetricKey(const uint8_t* key,
                           size_t keySize,
                           uint32_t algorithm,
                           SymmetricKeyId keyId,
                           uint32_t initializationCount)
    : _algorithm(algorithm),
      _keySize(keySize),
      _key(key, key + keySize),
      _keyId(std::move(keyId)),
      _initializationCount(initializationCount),
      _invocationCount(0) {
    if (_keySize < crypto::minKeySize || _keySize > crypto::maxKeySize) {
        error() << "Attempt to construct symmetric key of invalid size: " << _keySize;
        return;
    }
}

SymmetricKey::SymmetricKey(SecureVector<uint8_t> key, uint32_t algorithm, SymmetricKeyId keyId)
    : _algorithm(algorithm),
      _keySize(key->size()),
      _key(std::move(key)),
      _keyId(std::move(keyId)),
      _initializationCount(1),
      _invocationCount(0) {}

SymmetricKey::SymmetricKey(SymmetricKey&& sk)
    : _algorithm(sk._algorithm),
      _keySize(sk._keySize),
      _key(std::move(sk._key)),
      _keyId(std::move(sk._keyId)),
      _initializationCount(sk._initializationCount),
      _invocationCount(sk._invocationCount.load()) {}

SymmetricKey& SymmetricKey::operator=(SymmetricKey&& sk) {
    _algorithm = sk._algorithm;
    _keySize = sk._keySize;
    _key = std::move(sk._key);
    _keyId = std::move(sk._keyId);
    _initializationCount = sk._initializationCount;
    _invocationCount.store(sk._invocationCount.load());

    return *this;
}
}  // namespace mongo