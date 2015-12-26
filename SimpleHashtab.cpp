#include "SimpleHashtab.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>


Hashtab::Hashtab(unsigned int size): errno_(0)
{
    memset(&htab_, 0, sizeof(struct hsearch_data));

    if (hcreate_r(size, &htab_) == 0) {
        //errno_ = errno;
        errno_ = HT_ERR_CREATE_FAILED;
        //fprintf(stderr, "hcreate_r failed\n");
    }
}


Hashtab::~Hashtab()
{
    for (unsigned int i = 0; i <= htab_.size; i++) {
        if (htab_.table[i].used == 0
            || htab_.table[i].used == (unsigned int)-1)
        {
            continue;
        }

        if (htab_.table[i].entry.key != NULL) free(htab_.table[i].entry.key);
        // XXX what about entry.data?
    }

    if (errno_ != HT_ERR_CREATE_FAILED) hdestroy_r(&htab_);
}


// XXX
// life span of \param key
// memory ops of \param key and \param data
bool Hashtab::insert(const char *key, const void *data)
{
    if (key == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return false;
    }

    ENTRY e, *ep = NULL;
    size_t len = strlen(key);

    // XXX in spite of portability, strdup is preferred
    if ((e.key = (char *)malloc(len + 1)) == NULL) {
        errno_ = ENOMEM;
        //fprintf(stderr, "malloc failed for ENTRY.key\n");
        return false;
    }

    memcpy(e.key, key, len);
    e.key[len] = '\0';

    e.data = const_cast<void *>(data);

    if (hsearch2_r(e, ENTER2, &ep, &htab_, GC_NONE) == 0) {
        errno_ = errno;
        free(e.key);
        //fprintf(stderr, "hsearch_r ENTER failed\n");
        return false;
    }

    clearErrno();

    return true;
}


bool Hashtab::replace(const char *key, const void *data, GC_MANUAL manual)
{
    if (key == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return false;
    }

    ENTRY e, *ep = NULL;

    e.key = const_cast<char *>(key);
    e.data = const_cast<void *>(data);

    if (hsearch2_r(e, REPLACE, &ep, &htab_, manual) == 0) {
        errno_ = errno;
        //fprintf(stderr, "hsearch2_r REPLACE failed\n");
        return false;
    }

    clearErrno();

    return true;
}


ENTRY *Hashtab::find(const char *key)
{
    if (key == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return NULL;
    }

    ENTRY e, *ep = NULL;

    e.key = const_cast<char *>(key);

    if (hsearch2_r(e, FIND2, &ep, &htab_, GC_NONE) == 0) {
        errno_ = errno;
        //fprintf(stderr, "hsearch_r FIND failed\n");
        return NULL;
    }

    clearErrno();

    return ep;
}


void *Hashtab::findData(const char *key)
{
    return getEntryData(find(key));
}


bool Hashtab::erase(const char *key, GC_MANUAL manual)
{
    if (key == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return false;
    }

    ENTRY e, *ep = NULL;

    e.key = const_cast<char *>(key);

    if (hsearch2_r(e, RELEASE, &ep, &htab_, manual) == 0) {
        errno_ = errno;
        //fprintf(stderr, "hsearch2_r RELEASE failed\n");
        return false;
    }

    clearErrno();

    return true;
}


unsigned int Hashtab::getSize() const
{
    return htab_.size;
}


unsigned int Hashtab::getFilled() const
{
    return htab_.filled;
}


int Hashtab::getErrno() const
{
    return errno_;
}


int Hashtab::clearErrno()
{
    int orig = errno_;

    errno_ = 0;

    return orig;
}


bool Hashtab::getKeys(char *keys[], unsigned int num)
{
    if (num < getFilled()) {
        errno_ = HT_ERR_SHORT_SPACE;
        return false;
    }

    unsigned int n = 0;

    for (unsigned int i = 0; i <= htab_.size; i++) {
        if (htab_.table[i].used == 0
            || htab_.table[i].used == (unsigned int)-1)
        {
            continue;
        }

        keys[n++] = htab_.table[i].entry.key;
    }

    while (n < num) keys[n++] = NULL;

    clearErrno();

    return true;
}


const char *Hashtab::getEntryKey(const ENTRY *ep)
{
    if (ep == NULL || ep->key == NULL || ep->key[0] == '\0') {
        errno_ = HT_ERR_BAD_ARG;
        return NULL;
    }

    clearErrno();

    return const_cast<const char *>(ep->key);
}


void *Hashtab::getEntryData(const ENTRY *ep)
{
    if (ep == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return NULL;
    }

    clearErrno();

    return ep->data;
}


bool Hashtab::traverse(ht_walk_fn fn, int on_error)
{
    if (fn == NULL) {
        errno_ = HT_ERR_BAD_ARG;
        return false;
    }

    for (unsigned int i = 0; i <= htab_.size; i++) {
        if (htab_.table[i].used == 0
            || htab_.table[i].used == (unsigned int)-1)
        {
            continue;
        }

        if ((*fn)(htab_.table[i].entry.key, htab_.table[i].entry.data) < 0) {
            switch (on_error) {
            case 1:
                return false;
            default:
                break;
            }
        }
    }

    clearErrno();

    return true;
}
