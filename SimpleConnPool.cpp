#include "SimpleConnPool.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <stdint.h>
#include <math.h>

#ifndef UINT64_MAX
#define UINT64_MAX (18446744073709551615ULL)
#endif

conn_pool_t *conn_pool_create(unsigned int size)
{
    conn_pool_t *cp = NULL;

    if (size > CONN_MAX) {
        size = CONN_MAX;
        fprintf(stderr, "connection number must not exceed %d\n", CONN_MAX);
    }

    if ((cp = (conn_pool_t *)malloc(sizeof(conn_pool_t))) == NULL) {
        fprintf(stderr, "creating conn pool failed\n");
        return NULL;
    }

    if ((cp->socks = (int *)malloc(sizeof(int) * size)) == NULL) {
        free(cp);
        fprintf(stderr, "creating socket array failed\n");
        return NULL;
    }

    // XXX
    memset(cp->socks, -1, sizeof(int) * size);

    // TODO mutex attr & cond attr

    pthread_mutex_init(&cp->mutex, NULL);
    pthread_cond_init(&cp->cond, NULL);
    pthread_cond_init(&cp->cond2, NULL);

    cp->size = size;
    cp->num = 0;
    cp->avail = 0;

    return cp;
}


bool conn_pool_destroy(conn_pool_t *cp)
{
    int rc;

    if (cp == NULL) return true;

    // TODO mutex attr & cond attr

    rc = pthread_cond_destroy(&cp->cond);

    switch (rc) {
    case 0:
        break;
    // FIXME
    case EBUSY:
        fprintf(stderr, "some threads are currently waiting on the cond\n");
        return false;
    default:
        fprintf(stderr, "unknown error\n");
        return false;
    }

    rc = pthread_cond_destroy(&cp->cond2);

    switch (rc) {
    case 0:
        break;
    // FIXME
    case EBUSY:
        fprintf(stderr, "some threads are currently waiting on the cond2\n");
        return false;
    default:
        fprintf(stderr, "unknown error\n");
        return false;
    }

    rc = pthread_mutex_destroy(&cp->mutex);

    switch (rc) {
    case 0:
        break;
    case EBUSY:
        fprintf(stderr, "mutex is currently used\n");
        return false;
    default:
        fprintf(stderr, "unknown error\n");
        return false;
    }

    if (cp->socks != NULL) {
        for (unsigned int i = 0; i < cp->size; i++) {
            if (cp->socks[i] == -1) continue;
            close(cp->socks[i]);
        }

        free(cp->socks);
        cp->socks = NULL;
    }

    free(cp);
    cp = NULL;

    return true;
}


bool conn_pool_clear(conn_pool_t *cp)
{
    int rc;

    if (cp == NULL) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);

    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

    if (cp->socks != NULL) {
        for (unsigned int i = 0; i < cp->size; i++) {
            if (cp->socks[i] == -1) continue;
            close(cp->socks[i]);
            cp->socks[i] = -1;
        }
    }

    //cp->avail = (uint64_t)pow(2, cp->num / 4) - 1;
    //if (cp->avail == 0) cp->avail = 1;
    cp->avail = 0;
    cp->num = 0;

    pthread_mutex_unlock(&cp->mutex);

    return true;
}


int conn_pool_take(conn_pool_t *cp)
{
    int rc, sockfd;

    if (cp == NULL) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);

    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

#if 1
    //while (cp->num == cp->size && cp->avail == 0) {
    while (cp->num >= cp->size && cp->avail == 0) {
        pthread_cond_wait(&cp->cond, &cp->mutex);
    }
#endif

#if 0
    //if (cp->num == cp->size && cp->avail == 0) {
    if (cp->num >= cp->size && cp->avail == 0) {
        struct timespec ts;

        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 10;
        pthread_cond_timedwait(&cp->cond, &cp->mutex, &ts);

        if (cp->num >= cp->size && cp->avail == 0) {
            pthread_mutex_unlock(&cp->mutex);
            // XXX
            return CP_ERR_INADEQUATE;
        }
    }
#endif

    if (cp->avail > 0) {
        unsigned int i;

        for (i = 0; i < cp->size; i++) {
            if (cp->socks[i] == -1) continue;

            uint64_t mask = 1 << i;

            if (cp->avail & mask) {
                cp->avail &= ~mask;
                break;
            }
        }

        if (i < cp->size) {
            sockfd = cp->socks[i];
            pthread_mutex_unlock(&cp->mutex);
            return sockfd;
        }
    }

    pthread_mutex_unlock(&cp->mutex);

    return CP_ERR_INADEQUATE;
}


int conn_pool_put(conn_pool_t *cp, int sockfd)
{
    int rc;

    if (cp == NULL || sockfd < 0) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);

//fprintf(stderr, "pool info BEFORE put: cp[%p] socket[%d] size[%u] num[%u]\n", cp, sockfd, cp->size, cp->num);
//for (unsigned int i = 0; i < cp->size; i++)
//fprintf(stderr, "cp[%p] cp->socks[%d]: %d\n", cp, i, cp->socks[i]);

    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

    unsigned int i = 0, vacant = cp->size;

    for (; i < cp->size; i++) {
        if (vacant == cp->size && cp->socks[i] == -1) {
            vacant = i;
        }
        if (cp->socks[i] == sockfd) {
            break;
        }
    }

    // new socket, not taken from pool ever before
    if (i == cp->size) {
        if (cp->num == cp->size) {
            pthread_mutex_unlock(&cp->mutex);
            //close(sockfd);
            fprintf(stderr, "pool too full to hold new connections. "
                    "size[%u] num[%u] avail[%lx]\n",
                    cp->size, cp->num, cp->avail);
            return CP_ERR_FULL;
        }

        //if (vacant == cp->size) vacant = cp->num;
        cp->socks[vacant] = sockfd;
        cp->num++;
        cp->avail |= 1 << vacant;
    } else {
        //if (cp->num == cp->size && cp->avail == 0) {
        if (cp->num >= cp->size && cp->avail == 0) {
            pthread_cond_broadcast(&cp->cond);
        }
        cp->avail |= 1 << i;
    }

    pthread_mutex_unlock(&cp->mutex);

    return sockfd;
}


bool conn_pool_remove(conn_pool_t *cp, int sockfd)
{
    int rc;

    if (cp == NULL || sockfd < 0) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);
    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

    unsigned int i;

    for (i = 0; i < cp->size; i++) {
        if (cp->socks[i] != sockfd) continue;

        uint64_t mask = 1 << i;

        // XXX close inside or outside of pool?
        close(sockfd);
        cp->socks[i] = -1;
        cp->avail &= ~mask;
        cp->num--;
        if (cp->num + 1 >= cp->size && cp->avail == 0) {
            pthread_cond_broadcast(&cp->cond);
        }
    }

    if (i == cp->size) {
        fprintf(stderr, "no such socket[%d] existed in conn pool\n", sockfd);
    }

    pthread_mutex_unlock(&cp->mutex);

    return true;
}


// take a specific socket, by index of array
int conn_pool_take2(conn_pool_t *cp, unsigned int idx)
{
    int rc, sockfd;

    if (cp == NULL || idx >= cp->size) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);

    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

    // slot of required socket is still empty
    if (cp->socks[idx] == -1) {
//fprintf(stderr, "in conn_pool_take2. INADEQUATE. idx[%u] cp[%p] size[%u] num[%u]\n", idx, cp, cp->size, cp->num);
//for (unsigned int i = 0; i < cp->size; i++)
//fprintf(stderr, "cp[%p] cp->socks[%d]: %d\n", cp, i, cp->socks[i]);
//fprintf(stderr, "----------------------------------------------------------\n");
        pthread_mutex_unlock(&cp->mutex);
        return CP_ERR_INADEQUATE;
    }

    uint64_t mask = 1 << idx;

#if 1
    while ((cp->avail & mask) == 0) {
        pthread_cond_wait(&cp->cond2, &cp->mutex);
    }
#endif

#if 0
    if ((cp->avail & mask) == 0) {
        struct timespec ts;

        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 10;
        rc = pthread_cond_timedwait(&cp->cond2, &cp->mutex, &ts);

        if ((cp->avail & mask) == 0) {
//fprintf(stderr, "maybe cond wait timedout [%d]\n", rc);
            pthread_mutex_unlock(&cp->mutex);
            // XXX make use of temp sockets
            return CP_ERR_INADEQUATE;
        }
    }
#endif

    cp->avail &= ~mask;

    sockfd = cp->socks[idx];

    pthread_mutex_unlock(&cp->mutex);

    return sockfd;
}


int conn_pool_put2(conn_pool_t *cp, int sockfd, unsigned int idx)
{
    int rc;

    if (cp == NULL || idx >= cp->size || sockfd < 0) return CP_ERR_ARGS;

    rc = pthread_mutex_lock(&cp->mutex);
    switch (rc) {
    case 0:
        break;
    case EDEADLK:
        fprintf(stderr, "dead lock\n");
        return CP_ERR_LOCK;
    case EINVAL:
        fprintf(stderr, "mutex not initialized yet\n");
        return CP_ERR_LOCK;
    default:
        fprintf(stderr, "unknown error\n");
        return CP_ERR_LOCK;
    }

    uint64_t mask = 1 << idx;

    if ((cp->avail & mask) != 0) {
        pthread_mutex_unlock(&cp->mutex);
#if 0
        close(sockfd);
#endif
        fprintf(stderr, "pool too full to hold new connections. "
                "index[%u] avail[%lx]\n", idx, cp->avail);
        return CP_ERR_FULL;
    }

    cp->avail |= mask;

    // new socket, not taken from pool ever before
    if (cp->socks[idx] == -1) {
        cp->socks[idx] = sockfd;
        cp->num++;
    }

    pthread_cond_broadcast(&cp->cond2);

    pthread_mutex_unlock(&cp->mutex);

    return sockfd;
}


#if 0
// FIXME pretection
inline unsigned int conn_pool_size(conn_pool_t *cp)
{
    return cp->size;
}


inline unsigned int conn_pool_filled(conn_pool_t *cp)
{
    return cp->num;
}
#endif
