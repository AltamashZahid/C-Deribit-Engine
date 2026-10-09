#pragma once

#include <openssl/ssl.h>

namespace de::deribit {

// Adds the operating system's trusted root certificates to `ctx`. On Windows this
// copies the "ROOT" certificate store (OpenSSL can't read it by itself); elsewhere it
// uses OpenSSL's default verify paths. Returns the number of certificates added on
// Windows, or 1/0 for success/failure elsewhere.
int load_system_root_certificates(SSL_CTX* ctx);

}  // namespace de::deribit
