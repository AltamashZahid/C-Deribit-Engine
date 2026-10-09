#include "deribit/tls_roots.hpp"

#include <openssl/x509.h>

#ifdef _WIN32
// wincrypt.h must come after the OpenSSL headers: it #defines names such as X509_NAME
// that clash with OpenSSL's types.
#include <windows.h>
#include <wincrypt.h>
#endif

namespace de::deribit {

int load_system_root_certificates(SSL_CTX* ctx) {
#ifdef _WIN32
    HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
    if (!store) return 0;
    X509_STORE* x509_store = SSL_CTX_get_cert_store(ctx);
    int added = 0;
    PCCERT_CONTEXT cert = nullptr;
    while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
        const unsigned char* der = cert->pbCertEncoded;
        X509* x509 = d2i_X509(nullptr, &der, static_cast<long>(cert->cbCertEncoded));
        if (!x509) continue;
        if (X509_STORE_add_cert(x509_store, x509) == 1) ++added;
        X509_free(x509);
    }
    CertCloseStore(store, 0);
    return added;
#else
    return SSL_CTX_set_default_verify_paths(ctx) == 1 ? 1 : 0;
#endif
}

}  // namespace de::deribit
