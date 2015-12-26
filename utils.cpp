#include "utils.h"

#include "SimpleConf.h"
#include "SimpleHashtab.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <sys/time.h>
#include <dirent.h>
#include <poll.h>
#include <sys/stat.h>
#include <arpa/inet.h>


int g_ep_wait_timeout, g_follower_primary_port, g_follower_secondary_port,
    g_recv_timeout, g_send_timeout, g_broker_alive;
bool g_leader_on, g_follower_on, g_broker_slave_running;
char *g_follower_host, *g_zk_hosts, *g_data_dirs, *g_broker_id, *g_root_dir;
thr_pool_t *g_tpool;
conn_pool_t *g_cpool;
replica_inot_map_t g_inot_map;
zhandle_t *g_zh;

static Hashtab *g_topic_dir_map;
static replica_file_map_t g_file_map;


static void *rsync_data(void *arg);


bool init_config(const char *conf_file)
{
    if (conf_file == NULL) {
        fprintf(stderr, "Invalid conf file\n");
        return false;
    }

    Hashtab *replica_conf = load_conf(conf_file);
    if (replica_conf == NULL) {
        return false;
    }

    char *temp;

    temp = read_conf(replica_conf, "leader");
    if (temp == NULL || strcmp(temp, "off") == 0) {
        g_leader_on = false;
    } else if (strcmp(temp, "on") == 0) {
        g_leader_on = true;
    } else {
        fprintf(stderr, "Invalid value[%s] for "
                "\"leader\" configuration item\n", temp);
        destroy_conf(replica_conf);
        return false;
    }

    temp = read_conf(replica_conf, "follower");
    if (temp == NULL || strcmp(temp, "off") == 0) {
        g_follower_on = false;
    } else if (strcmp(temp, "on") == 0) {
        g_follower_on = true;
    } else {
        fprintf(stderr, "Invalid value[%s] for "
                "\"follower\" configuration item\n", temp);
        destroy_conf(replica_conf);
        return false;
    }

    if (g_leader_on) {
        g_follower_host = read_conf(replica_conf, "follower_host");
        if (g_follower_host == NULL) {
            destroy_conf(replica_conf);
            return false;
        }
    }

    if (g_follower_on) {
        // millisecond
        temp = read_conf(replica_conf, "epoll_wait_timeout");
        if (temp == NULL) {
            fprintf(stderr, "Invalid epoll wait timeout\n");
            destroy_conf(replica_conf);
            return false;
        }

        g_ep_wait_timeout = atoi(temp);

        g_zk_hosts = read_conf(replica_conf, "zookeeper_hosts");
        if (g_zk_hosts == NULL) {
            fprintf(stderr, "Invalid Zookeeper hosts\n");
            destroy_conf(replica_conf);
            return false;
        }

        g_data_dirs = read_conf(replica_conf, "data_dirs");
    }

    g_broker_id = read_conf(replica_conf, "broker_id");
    if (g_broker_id == NULL) {
        fprintf(stderr, "Ivalid broker id\n");
        destroy_conf(replica_conf);
        return false;
    }

    g_root_dir = read_conf(replica_conf, "broker_data_dir");
    if (g_root_dir == NULL) {
        destroy_conf(replica_conf);
        return false;
    }

    temp = read_conf(replica_conf, "follower_primary_port");
    if (temp == NULL) {
        destroy_conf(replica_conf);
        return false;
    }

    g_follower_primary_port = atoi(temp);

    temp = read_conf(replica_conf, "follower_secondary_port");
    if (temp == NULL) {
        destroy_conf(replica_conf);
        return false;
    }

    g_follower_secondary_port = atoi(temp);

    // millisecond
    temp = read_conf(replica_conf, "recv_timeout");
    if (temp == NULL) {
        destroy_conf(replica_conf);
        return false;
    }

    g_recv_timeout = atoi(temp);

    // millisecond
    temp = read_conf(replica_conf, "send_timeout");
    if (temp == NULL) {
        destroy_conf(replica_conf);
        return false;
    }

    g_send_timeout = atoi(temp);

    return true;
}


bool build_topic_dir_map()
{
    if (g_data_dirs == NULL || g_data_dirs[0] == '\0') {
        fprintf(stderr, "Invalid data_dirs config\n");
        return false;
    }

    int count = 1, colons[64] = {0}, commas[64] = {0};

    for (int i = 0, j = 0, k = 0; g_data_dirs[i] != '\0'; i++) {
        if (g_data_dirs[i] == ':') {
            colons[j++] = i;
            g_data_dirs[i] = '\0';
        } else if (g_data_dirs[i] == ',') {
            commas[k++] = i;
            g_data_dirs[i] = '\0';
            count++;
        }
    }

    bool ret;
    char *k, *v;

    g_topic_dir_map = new Hashtab(count * 2 + 1);

    if (g_topic_dir_map->getErrno() != 0) {
        ret = false;
        goto rtn;
    }

    if (!g_topic_dir_map->insert(g_data_dirs, g_data_dirs + colons[0] + 1)) {
        fprintf(stderr, "SimpleHashtab::insert[%s][%s] failed\n",
                g_data_dirs, g_data_dirs + colons[0] + 1);
        ret = false;
        goto rtn;
    }

    for (int i = 0; commas[i] > 0; i++) {
        k = g_data_dirs + commas[i] + 1;
        v = g_data_dirs + colons[i + 1] + 1;
        if (!g_topic_dir_map->insert(k, v)) {
            fprintf(stderr, "SimpleHashtab::insert[%s][%s] failed\n", k, v);
            ret = false;
            goto rtn;
        }
    }

    return true;

rtn:
    delete g_topic_dir_map;
    g_topic_dir_map = NULL;
    return ret;
}


/*
SO_REVBUF SO_SNDBUF
SO_REVTIMEO SO_SNDTIMEO
*/
int init_socket(short int port, int backlog)
{
    int sfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sfd == -1) {
        fprintf(stderr, "socket failed with errno[%d]\n", errno);
        return -1;
    }

    // SO_REUSEADDR
    int flag = 1;

    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR,
                   (const void *)&flag, sizeof(int)) == -1)
    {
        fprintf(stderr, "setsockopt failed with errno[%d]\n", errno);
        goto rtn;
    }

    if (set_nonblocking(sfd) == -1) {
        fprintf(stderr, "Setting socket nonblocking failed with errno[%d]\n",
                errno);
        goto rtn;
    }

    struct sockaddr_in loc_addr;

    memset(&loc_addr, 0, sizeof(loc_addr));
    loc_addr.sin_family = AF_INET;
    loc_addr.sin_addr.s_addr = INADDR_ANY;
    loc_addr.sin_port = htons(port);

    if (bind(sfd, (struct sockaddr *)&loc_addr, sizeof(loc_addr)) == -1) {
        fprintf(stderr, "bind failed with errno[%d]\n", errno);
        goto rtn;
    }

    if (listen(sfd, backlog) == -1) {
        fprintf(stderr, "listen failed with errno[%d]\n", errno);
        goto rtn;
    }

    return sfd;

rtn:
    if (close(sfd) == -1) {
        fprintf(stderr, "close[%d] failed with errno[%d]\n", sfd, errno);
    }

    return -1;
}


enum {
    EN_UNKNOWN = 0,
    EN_LITTLE,
    EN_BIG
};


// num is from network (big-endian)
static uint64_t ntohll(uint64_t num)
{
    // endian of local host
    static int endian = EN_UNKNOWN;

    union {
        uint64_t ui64;
        unsigned char uc[8];
    } x;

    unsigned char temp;

    if (endian == EN_UNKNOWN) {
        x.ui64 = 0x01;
        endian = x.uc[7] == 0x01 ? EN_BIG : EN_LITTLE;
    }

    if (endian == EN_BIG) {
        //printf("big endian\n");
        return num;
    }

    //printf("small endian\n");

    for (int i = 0; i < 8 / 2; i++) {
        temp = x.uc[i];
        x.uc[i] = x.uc[7 - i];
        x.uc[7 - i] = temp;
    }

    return x.ui64;
}


// num is from local host (endian TBD)
static uint64_t htonll(uint64_t num)
{
    return ntohll(num);
}


// [total len: 4][dir name: 64][file name: 32][offset: 8][data]
static replica_sync_data_t *pack_data(replica_file_t *f,
                                      char *data, uint32_t mlen)
{
    if (f == NULL) return NULL;

    uint32_t l = 4 + 64 + 32 + 8 + (data != NULL ? mlen : 0);
    replica_sync_data_t *d;
    long offset;

    d = (replica_sync_data_t *)calloc(1, sizeof(replica_sync_data_t) + l);
    if (d == NULL) {
        fprintf(stderr, "calloc for packing data failed\n");
        return NULL;
    }

    d->len = l;

    l = htonl(l);
    offset = (long)htonll((uint64_t)f->offset);

    memcpy(d->data, (char *)&l, 4); // total len
    memcpy(d->data + 4, f->dn, f->dnl); // dir name
    memcpy(d->data + 4 + 64, f->fn, strlen(f->fn)); // file name
    memcpy(d->data + 4 + 64 + 32, (char *)&offset, 8);
    if (data != NULL && mlen > 0) {
        memcpy(d->data + 4 + 64 + 32 + 8, data, mlen); // data
    }

    return d;
}


inline void reclaim_data(replica_sync_data_t *d)
{
    free(d);
}


static int connect_peer(const char *host, short int port)
{
    if (host == NULL) return -1;

    struct addrinfo hint, *r, *aip;
    char serv[8];

    hint.ai_flags = AI_ADDRCONFIG | AI_NUMERICSERV;
    hint.ai_family = AF_INET;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_protocol = 0;

    snprintf(serv, sizeof(serv), "%hd", port);

    int rc = getaddrinfo(host, serv, &hint, &r);

    if (rc != 0) {
        fprintf(stderr, "getaddrinfo[%s:%s] failed with error[%s]\n",
                host, serv, gai_strerror(rc));
        return -1;
    }

    int sockfd = -1;
    struct timeval rtimeout, stimeout;

    rtimeout.tv_sec = g_recv_timeout / 1000;
    rtimeout.tv_usec = 0;
    stimeout.tv_sec = g_send_timeout / 1000;
    stimeout.tv_usec = 0;

    for (aip = r; aip != NULL; aip = aip->ai_next) {
        sockfd = socket(aip->ai_family, aip->ai_socktype, aip->ai_protocol);

        if (sockfd == -1) {
            fprintf(stderr, "socket failed with errno[%d]\n", errno);
            continue;
        }

        // TODO more options
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO,
                       &rtimeout, sizeof(struct timeval)) == -1)
        {
            fprintf(stderr, "setsockopt RCVTIMEO failed with errno[%d]\n",
                    errno);
            close(sockfd);
            continue;
        }
        if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO,
                       &stimeout, sizeof(struct timeval)) == -1)
        {
            fprintf(stderr, "setsockopt SNDTIMEO failed with errno[%d]\n",
                    errno);
            close(sockfd);
            continue;
        }

        int i;
        for (i = 0; i < 3; i++) {
            if (connect(sockfd, aip->ai_addr, aip->ai_addrlen) == 0) {
                break;
            }
            fprintf(stderr, "connect[%s:%s] failed with errno[%d]\n",
                    host, serv, errno);
            usleep(100000 * (1 << i));
            continue;
        }
        if (i == 3) {
            close(sockfd);
            continue;
        }

        break;
    }

    freeaddrinfo(r);

    return sockfd;
}


ssize_t sendall(int sockfd, const void *buf, size_t len, int flags)
{
    ssize_t n, total = 0;
    const char *p = (const char *)buf;

    while (len > 0) {
        int i;

        for (i = 0; i < 3; i++) {
            if ((n = send(sockfd, p + total, len, flags)) >= 0) {
                break;
            }
            fprintf(stderr, "send failed with errno[%d] and was to retry[%d]",
                    errno, i + 1);
            usleep(500000 * (1 << i));
        }

        if (i == 3) {
            return n;
        }

        total += n;
        len -= n;
    }

    return total;
}


ssize_t recvall(int sockfd, void *buf, size_t len, int flags)
{
    ssize_t n, total = 0;
    char *p = (char *)buf;

    while (total < (ssize_t)len) {
        int i;

        for (i = 0; i < 3; i++) {
            if ((n = recv(sockfd, p + total, len - total, flags)) == -1) {
                fprintf(stderr, "recv failed with errno[%d] and "
                        "was to retry[%d]\n", errno, i + 1);
                usleep(500000 * (1 << i));
                continue;
            } else if (n == 0) {
                fprintf(stderr, "recv failed for peer shutdown\n");
                return n;
            } else {
                break;
            }
        }

        if (i == 3) {
            return n;
        }

        total += n;
    }

    return total;
}


static uint32_t tell_follower(uint32_t what)
{
    int sfd = connect_peer(g_follower_host, g_follower_secondary_port);
    if (sfd == -1) {
        fprintf(stderr, "tell_follower[%s:%hd] failed\n",
                g_follower_host, g_follower_secondary_port);
        return -1;
    }

    uint32_t w = htonl(what);

    if (sendall(sfd, (char *)&w, sizeof(uint32_t), 0) != sizeof(uint32_t)) {
        fprintf(stderr, "sendall[%s/%hd] failed\n",
                g_follower_host, g_follower_secondary_port);
        close(sfd);
        return -1;
    }

    char buf[5];

    if (recvall(sfd, buf, 4, 0) != 4) {
        fprintf(stderr, "recvall[%s/%hd] failed\n",
                g_follower_host, g_follower_secondary_port);
        close(sfd);
        return -1;
    }

    buf[4] = '\0';
    w = ntohl(uint32_t(atoi(buf)));
    close(sfd);

    return w;
}


int start_broker(int role, const char *broker_id)
{
    char cmd[64];

    switch (role) {
    case ROLE_LEADER:
        snprintf(cmd, sizeof(cmd), "%s-%s start",
                "/etc/init.d/qbus-broker-master", broker_id);
        break;
    case ROLE_FOLLOWER:
        if (g_broker_slave_running) {
            fprintf(stderr, "broker[%s] slave is already running\n", broker_id);
            return 0;
        }
        snprintf(cmd, sizeof(cmd), "%s-%s start",
                "/etc/init.d/qbus-broker-slave", broker_id);
        break;
    default:
        fprintf(stderr, "Invalid role\n");
        return -1;
    }

    FILE *pp = popen(cmd, "re");
    if (pp == NULL) {
        fprintf(stderr, "popen failed with errno[%d]\n", errno);
        return -1;
    }

    sleep(1);

    if (pclose(pp) == -1) {
        fprintf(stderr, "pclose failed with errno[%d]\n", errno);
        return -1;
    }

    if (role == ROLE_FOLLOWER) {
        g_broker_slave_running = true;
    }

    return 0;
}


static off_t get_fsize_by_fd(int fd)
{
    struct stat buf;

    if (fstat(fd, &buf) == -1) {
        fprintf(stderr, "lstat[fd:%d] failed with errno[%d]\n", fd, errno);
        return (off_t)-1;
    }

    return buf.st_size;
}


static off_t get_fsize_by_path(const char *path)
{
    struct stat buf;

    if (stat(path, &buf) == -1) {
        fprintf(stderr, "stat[%s] failed with errno[%d]\n", path, errno);
        return (off_t)-1;
    }

    return buf.st_size;
}


static file_tail_t *tail_file(replica_file_t *f)
{
    off_t curr_offset = lseek(f->fd, f->offset, SEEK_SET);
    if (curr_offset == (off_t)-1) {
        fprintf(stderr, "lseek[%s/%s/%s] to [%ld] failed with errno[%d]\n",
                g_root_dir, f->dn, f->fn, f->offset, errno);
        return NULL;
    }

    off_t fsize = get_fsize_by_fd(f->fd);
    if (fsize == (off_t)-1) {
        fprintf(stderr, "get_file_size[%s/%s/%s] failed\n",
                g_root_dir, f->dn, f->fn);
        return NULL;
    }

    file_tail_t *t = (file_tail_t *)malloc(sizeof(file_tail_t)
                                           + fsize - curr_offset);
    if (t == NULL) {
        fprintf(stderr, "malloc failed for [%s/%s/%s]'s tail\n",
                g_root_dir, f->dn, f->fn);
        return NULL;
    }

    ssize_t len = read(f->fd, t->buf, fsize - curr_offset);
    if (len == -1) {
        fprintf(stderr, "read[%s/%s/%s] failed with errno[%d]\n",
                g_root_dir, f->dn, f->fn, errno);
        free(t);
        return NULL;
    }

    t->len = (uint32_t)len;

    return t;
}


static inline void free_tail(file_tail_t *t)
{
    free(t);
}


static int handle_follower_response(uint32_t code,
                                    replica_sync_t *payload, int sockfd)
{
    switch (code) {
    case 0:
        payload->f->offset += payload->noff;
        break;

    // living replica leader restores broker master's data
    // case 1: broker master crashed while replica leader alive
    // case 2: machine broken
    case 1:
    {
// [total len: 8][file amount: 4]
// [data len: 8][data len: 8]
// [file name: 32][file name: 32][data][data]
        char buf[33], saved;

        if (recvall(sockfd, buf, 12, 0) != 12) {
            return -1;
        }

        buf[12] = '\0';
        saved = buf[8];
        buf[8] = '\0';

        uint64_t totalen;
        uint32_t nfiles;

        totalen = ntohll(strtoull(buf, NULL, 10));
        buf[8] = saved;
        nfiles = ntohl((uint32_t)atoi(buf + 8));

        char *buf2 = (char *)malloc(totalen - 12);
        if (buf2 == NULL) {
            return -1;
        }

        if (recvall(sockfd, buf2, totalen - 12, 0) != (ssize_t)totalen - 12) {
            free(buf2);
            return -1;
        }

        off_t *lens = (off_t *)malloc(nfiles);
        if (lens == NULL) {
            free(buf2);
            return -1;
        }

        // lengths of data
        for (uint32_t i = 0; i < nfiles; i++) {
            saved = buf2[(i + 1) * 8];
            buf2[(i + 1) * 8] = '\0';
            lens[i] = (off_t)ntohll(strtoull(buf2, NULL, 10));
            buf2[(i + 1) * 8] = saved;
        }

        char path[256], *p;

        for (uint32_t i = 0; i < nfiles; i++) {
            // names of files
            memcpy(buf, buf2 + 8 * nfiles + i * 32, 32);
            buf[32] = '\0';
            if (strcmp(buf, payload->f->fn) != 0) {
                close(payload->f->fd);
                snprintf(path, sizeof(path), "%s/%s/%s",
                         g_root_dir, payload->f->dn, payload->f->fn);
                if ((payload->f->fd = open(path, O_RDONLY | O_CREAT)) == -1) {
                    fprintf(stderr, "open[%s] failed with errno[%d]\n",
                            path, errno);
                    free(buf2);
                    free(lens);
                    return -1;
                }
                payload->f->offset = 0;
                memcpy(payload->f->fn, buf, strlen(buf));
            } else {
                if (lseek(payload->f->fd, payload->f->offset, SEEK_SET)
                    == (off_t) -1)
                {
                    free(buf2);
                    free(lens);
                    return -1;
                }
            }

            // p indicats beginning of current data
            if (i == 0) {
                p = buf2 + nfiles * (8 + 32);
            } else {
                p += lens[i - 1];
            }
            if (write(payload->f->fd, p, lens[i]) != lens[i]) {
                free(buf2);
                free(lens);
                return -1;
            }
            payload->f->offset += lens[i];
        }

        break;
    }

    case 2: // replica crashed while broker master alive all the time
    {
// [file/offset amount: 4]
// [dn: 64][fn: 32][offset: 8]
// [dn: 64][fn: 32][offset: 8]
        char buf[33];
        uint32_t noffsets;

        if (recvall(sockfd, buf, 4, 0) != 4) {
            return -1;
        }

        buf[4] = '\0';
        noffsets = ntohl((uint32_t)atoi(buf));

        char *buf2 = (char *)malloc(noffsets * (64 + 32 + 8));
        if (buf2 == NULL) {
            return -1;
        }

        char path[256], saved, *p;
        off_t offset;

        for (uint32_t i = 0; i < noffsets; i++) {
            // offsets of files
            saved = buf2[(i + 1) * (64 + 32 + 8)];
            buf2[(i + 1) * (64 + 32 + 8)] = '\0';
            p = buf2 + i * (64 + 32 + 8) + 64 + 32;
            offset = (off_t)ntohll(strtoull(p, NULL, 10));
            buf2[(i + 1) * (64 + 32 + 8)] = saved;

            // names of files
            p = buf2 + i * (64 + 32 + 8) + 64;
            memcpy(buf, p, 32);
            buf[32] = '\0';
            if (strcmp(payload->f->fn, buf) == 0) {
                payload->f->offset = offset;
            } else {
                close(payload->f->fd);
                snprintf(path, sizeof(path), "%s/%s/%s",
                         g_root_dir, payload->f->dn, buf);
                if ((payload->f->fd = open(path, O_RDONLY | O_CREAT)) == -1) {
                    fprintf(stderr, "open[%s] failed with errno[%d]\n", path, errno);
                    free(buf2);
                    return -1;
                }
                memcpy(payload->f->fn, p, strlen(p));
                payload->f->offset = offset;
            }

            file_tail_t *t = tail_file(payload->f);
            if (t == NULL) {
                fprintf(stderr, "tail_file[%s/%s/%s] failed\n",
                        g_root_dir, payload->f->dn, payload->f->fn);
                free(buf2);
                return -1;
            }

            replica_sync_data_t *d = pack_data(payload->f, t->buf, t->len);
            if (d == NULL) {
                fprintf(stderr, "pack_data failed for file[%s/%s/%s]\n",
                        g_root_dir, payload->f->dn, payload->f->fn);
                free_tail(t);
                free(buf2);
                return -1;
            }

            free_tail(t);

            replica_sync_t targ = { g_cpool, d, payload->f, d->len };

            if (thr_pool_queue(g_tpool, rsync_data, &targ) == -1) {
                fprintf(stderr, "thr_pool_queue failed with errno[%d]\n", errno);
                reclaim_data(d);
                free(buf2);
                return -1;
            }
        }

        break;
    }

    default:
        fprintf(stderr, "Exceptional follower response code[%d]\n", code);
        break;
    }

    return 0;
}


static void *rsync_data(void *arg)
{
    replica_sync_t *p = (replica_sync_t *)arg;
    int sfd = conn_pool_take(p->cp), i;

    if (sfd < 0) {
        if (sfd != CP_ERR_INADEQUATE) {
            fprintf(stderr, "conn_pool_take failed with code[%d]\n", sfd);
            return (void *)-1;
        }

        for (i = 0; i < 3; i++) {
            if ((sfd = connect_peer(g_follower_host, g_follower_primary_port))
                >= 0)
            {
                break;
            }
            usleep(100000 * (1 << i));
        }
        if (i == 3) {
            fprintf(stderr, "connect_peer[%s:%hd] failed\n",
                    g_follower_host, g_follower_primary_port);
            conn_pool_put(p->cp, sfd);
            return (void *)-1;
        }
    }

    if (sendall(sfd, p->d->data, p->d->len, 0) != p->d->len) {
        fprintf(stderr, "sendall failed with errno[%d]\n", errno);
        conn_pool_put(p->cp, sfd);
        return (void *)-1;
    }

    char buf[5];
    uint32_t resp_code;

    if (recvall(sfd, buf, 4, 0) != 4) {
        fprintf(stderr, "recvall failed to recv follower's response code\n");
        reclaim_data(p->d);
        p->d = NULL;
        conn_pool_put(p->cp, sfd);
        return (void *)-1;
    }

    reclaim_data(p->d);
    p->d = NULL;

    buf[4] = '\0';
    resp_code = ntohl(atoi(buf));

    handle_follower_response(resp_code, p, sfd);

    conn_pool_put(p->cp, sfd);

    return (void *)0;
}


enum {
    RLEAD_BROKER_READY = 1
};


// 1) machine broken
// 2) broker crashed while replica alive
static void handle_broker_revival(int sig)
{
    (void)sig;

    replica_inot_map_t::iterator it = g_inot_map.begin();

    while (it != g_inot_map.end()) {
        replica_sync_data_t *d = pack_data(it->second, NULL, 0);
        if (d == NULL) {
            fprintf(stderr, "pack_data failed for file[%s/%s/%s]\n",
                    g_root_dir, it->second->dn, it->second->fn);
            //return;
            continue;
        }

        replica_sync_t targ = { g_cpool, d, it->second, d->len };

        if (thr_pool_queue(g_tpool, rsync_data, &targ) == -1) {
            fprintf(stderr, "thr_pool_queue failed with errno[%d]\n", errno);
            reclaim_data(d);
            //return;
            continue;
        }

        it++;
    }

    if (tell_follower(RLEAD_BROKER_READY) != 0) return;

    // XXX signal reentrancy
    start_broker(ROLE_LEADER, g_broker_id);
}


int init_sig_handlers(int sig)
{
    struct sigaction act;

    act.sa_handler = handle_broker_revival;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
#ifdef SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;
#endif

    if (sigaction(sig, &act, NULL) == -1) {
        fprintf(stderr, "sigaction[%d] failed with errno[%d]\n", sig, errno);
        return -1;
    }

    return 0;
}


int prefill_conn_pool(conn_pool_t *cp, int num)
{
    if (cp == NULL) return -1;

    int sfd, n, rc;

    for (n = 0; n < num; n++) {
        int i;
        for (i = 0; i < 3; i++) {
            if ((sfd = connect_peer(g_follower_host, g_follower_primary_port))
                >= 0)
            {
                break;
            }
            usleep(100000 * (1 << i));
        }
        if (i == 3) {
            fprintf(stderr, "connect_peer[%s:%hd] failed\n",
                    g_follower_host, g_follower_primary_port);
            continue;
        }

        if ((rc = conn_pool_put(cp, sfd)) < 0) {
            fprintf(stderr, "conn_pool_put[socket:%d] failed with code[%d]\n",
                    sfd, rc);
            close(sfd);
        }
    }

    return n;
}


int init_watches(int inotify_fd, replica_inot_map_t& rmap)
{
    DIR *rootdp = opendir(g_root_dir);
    if (rootdp == NULL) {
        fprintf(stderr, "opendir[%s] failed with errno[%d]\n",
                g_root_dir, errno);
        return -1;
    }

    struct dirent *dep;
    char temp_path[512];
    int n = 0;

    errno = 0;

    // Create initial watches
    while ((dep = readdir(rootdp)) != NULL) {
        if (dep->d_type != DT_DIR || strcmp(dep->d_name, ".") == 0) {
            continue;
        }

        snprintf(temp_path, sizeof(temp_path), "%s/%s",
                 g_root_dir, dep->d_name);

        int wd = inotify_add_watch(inotify_fd, temp_path,
                                   IN_CREATE | IN_MODIFY
                                   | IN_Q_OVERFLOW);
        if (wd == -1) {
            fprintf(stderr, "inotify_add_watch[%s] failed "
                    "with errno[%d]\n", temp_path, errno);
            closedir(rootdp);
            return -1;
        }

        uint32_t l = strlen(dep->d_name);
        replica_file_t *f;

        f = (replica_file_t *)malloc(sizeof(replica_file_t) + l + 1);
        if (f == NULL) {
            fprintf(stderr, "malloc for [%s] failed\n", dep->d_name);
            // FIXME
            inotify_rm_watch(inotify_fd, wd);
            closedir(rootdp);
            return -1;
        }

        f->fd = -1;
        f->offset = 0;
        f->dnl = l;
        memcpy(f->dn, dep->d_name, l + 1);

        rmap[wd] = f;

        n++;
    }

    // XXX
    if (errno != 0) {
        fprintf(stderr, "readdir[%s] failed with errno[%d]\n",
                g_root_dir, errno);
        closedir(rootdp);
        return -1;
    }

    closedir(rootdp);

    return n;
}


static bool handle_ev_(const struct inotify_event *evp)
{
    replica_inot_map_t::iterator it = g_inot_map.find(evp->wd);

    // Should not hit here
    if (it == g_inot_map.end()) {
        fprintf(stderr, "Unexpected watch descriptor[%d]\n", evp->wd);
        return false;
    }

    char path[256];

    snprintf(path, sizeof(path), "%s/%s/%s",
             g_root_dir, it->second->dn, evp->name);

    // 1st time to handle this file since replica leader started up
    if (it->second->fd == -1) {
        if ((it->second->fd = open(path, O_RDONLY)) == -1) {
            fprintf(stderr, "open[%s] failed with errno[%d]\n", path, errno);
            return false;
        }
        it->second->offset = 0;
        memcpy(it->second->fn, evp->name, strlen(evp->name) + 1);
    } else if (strcmp(it->second->fn, evp->name) != 0) {
    // data log file changed (newer file created)
    //
    // FIXME
    // in the case that replica follower was broken for a long time
    // although this case is unlikely for replica monitoring scripts
        if (close(it->second->fd) == -1) {
            fprintf(stderr, "close[%s] failed with errno[%d]\n",
                    it->second->fn, errno);
            // noop
        }
        if ((it->second->fd = open(path, O_RDONLY)) == -1) {
            fprintf(stderr, "open[%s] failed with errno[%d]\n", path, errno);
            return false;
        }
        it->second->offset = 0;
        memcpy(it->second->fn, evp->name, strlen(evp->name) + 1);
    }

    file_tail_t *t = tail_file(it->second);
    if (t == NULL) {
        fprintf(stderr, "tail_file[%s/%s/%s] failed\n",
                g_root_dir, it->second->dn, it->second->fn);
        return false;
    }

    replica_sync_data_t *d = pack_data(it->second, t->buf, t->len);
    if (d == NULL) {
        fprintf(stderr, "pack_data failed for file[%s/%s/%s]\n",
                g_root_dir, it->second->dn, it->second->fn);
        free_tail(t);
        return false;
    }

    free_tail(t);

    replica_sync_t targ = { g_cpool, d, it->second, d->len };

    if (thr_pool_queue(g_tpool, rsync_data, &targ) == -1) {
        fprintf(stderr, "thr_pool_queue failed with errno[%d]\n", errno);
        reclaim_data(d);
        return false;
    }

    return true;
}


static bool handle_ev_modify(const struct inotify_event *evp)
{
    return handle_ev_(evp);
}


#define FNAME_TWENTY0S "00000000000000000000.kafka"


static bool handle_0_file(replica_file_t *f)
{
    char fui64path[256];

    snprintf(fui64path, sizeof(fui64path), "%s/%s/%s",
             g_root_dir, f->dn, f->fn);

    if ((f->fd = open(fui64path, O_RDONLY)) == -1) {
        fprintf(stderr, "open[%s] failed with errno[%d]\n", fui64path, errno);
        return false;
    }

    file_tail_t *t = tail_file(f);
    if (t == NULL) {
        fprintf(stderr, "tail_file[%s/%s/%s] failed\n",
                g_root_dir, f->dn, f->fn);
        return false;
    }

    replica_sync_data_t *d = pack_data(f, t->buf, t->len);
    if (d == NULL) {
        fprintf(stderr, "pack_data failed for file[%s/%s/%s]\n",
                g_root_dir, f->dn, f->fn);
        free_tail(t);
        return false;
    }

    free_tail(t);

    replica_sync_t targ = { g_cpool, d, f, d->len };

    if (thr_pool_queue(g_tpool, rsync_data, &targ) == -1) {
        fprintf(stderr, "thr_pool_queue failed with errno[%d]\n", errno);
        reclaim_data(d);
        return false;
    }

    return true;
}


static bool handle_ev_create(int inotify_fd, const struct inotify_event *evp)
{
    replica_inot_map_t::iterator it = g_inot_map.find(evp->wd);

    // Should not hit here
    if (it == g_inot_map.end()) {
        fprintf(stderr, "Unexpected watch descriptor[%d]\n", evp->wd);
        return false;
    }

    // files created
    if (evp->mask & IN_ISDIR == 0) return handle_ev_(evp);

    // directories created
    char temp_path[128];

    snprintf(temp_path, sizeof(temp_path), "%s/%s", g_root_dir, evp->name);

    int wd = inotify_add_watch(inotify_fd, temp_path,
                               IN_CREATE | IN_MODIFY
                               | IN_Q_OVERFLOW);
    if (wd == -1) {
        fprintf(stderr, "inotify_add_watch for [%s] failed "
                "with errno[%d]\n", temp_path, errno);
        return false;
    }

    uint32_t l = strlen(evp->name);
    replica_file_t *f;

    f = (replica_file_t *)malloc(sizeof(replica_file_t) + l + 1);
    if (f == NULL) {
        fprintf(stderr, "malloc for [%s] failed\n", temp_path);
        inotify_rm_watch(inotify_fd, wd);
        return false;
    }

    f->fd = -1;
    f->offset = 0;
    f->dnl = l;
    memcpy(f->dn, evp->name, l + 1);
    memcpy(f->fn, FNAME_TWENTY0S, sizeof(FNAME_TWENTY0S));

    g_inot_map[wd] = f;

    return handle_0_file(f);
}


static bool handle_event(int inotify_fd, const struct inotify_event *p)
{
    if (p->mask & IN_MODIFY) {
        return handle_ev_modify(p);
    } else if (p->mask & IN_CREATE) {
        return handle_ev_create(inotify_fd, p);
    }

    return false;
}


bool handle_events(int inotify_fd)
{
    struct pollfd fds[1];

    fds[0].fd = inotify_fd;
    fds[0].events = POLLIN;

    while (1) {
        int ret = poll(fds, 1, -1);

        if (ret == -1) {
            if (errno == EINTR) continue;
            fprintf(stderr, "poll failed with errno[%d]\n", errno);
            return false;
        }

        // Should not hit here
        if (ret == 0 || fds[0].revents & POLLIN == 0) {
            fprintf(stderr, "poll ret 0 or not POLLIN\n");
            continue;
        }

        char buf[8192]
            __attribute__ ((aligned(__alignof__(struct inotify_event))));

        ssize_t len = read(inotify_fd, buf, sizeof(buf));

        if (len == -1) {
            if (errno != EAGAIN) continue;
            fprintf(stderr, "read failed with errno[%d]\n", errno);
            return false;
        }

        // Should not hit here
        if (len == 0) {
            fprintf(stderr, "read inotify fd end\n");
            break;
        }

        const struct inotify_event *evp = NULL;

        for (const char *p = buf; p < buf + len;
             p += sizeof(struct inotify_event) + evp->len)
        {
            evp = (const struct inotify_event *)p;
            handle_event(inotify_fd, evp);
        }

        thr_pool_wait(g_tpool);
    }

    return true;
}


void do_watch(zhandle_t *zh, int type, int stat, const char *path, void *wctx)
{
    if (type == ZOO_SESSION_EVENT && stat == ZOO_EXPIRED_SESSION_STATE) {
        if (zh) {
            zookeeper_close(zh);
        }

        // FIXME
        if ((g_zh = zookeeper_init(g_zk_hosts, do_watch, 5, NULL, wctx, 0))
            == NULL)
        {
            fprintf(stderr, "zookeeper_init[%s] failed with errno[%d]\n",
                    g_zk_hosts, errno);
        }

        return;
    }

    // XXX
    if (strncmp(path, "/brokers/ids/", sizeof("/brokers/ids/")) != 0) return;

    int rc = zoo_exists(zh, path, 1, NULL);

    switch (rc) {
    case ZOK:
        g_broker_alive = 1;
        break;
    case ZNONODE:
        g_broker_alive = 0;
    default:
        fprintf(stderr, "zoo_exists[%s] failed with err[%d][%s]\n",
                path, rc, zerror(rc));
    }

    if (type == ZOO_CREATED_EVENT) {
        g_broker_alive = 1;
    } else if (type == ZOO_DELETED_EVENT) {
        g_broker_alive = 0;
    } else {
        fprintf(stderr, "Unexpected watch event[%d]\n", type);
    }
}


static replica_file2_t *prepare_write(const char *dn, const char *fn,
                                      off_t offset, int sfd)
{
    char path[256];
    replica_file_map_t::iterator it = g_file_map.find(std::string(dn));

    if (it == g_file_map.end()) {
        int n = snprintf(path, sizeof(path), "%s/%s/", g_root_dir, dn);
        if (mkdir(path, 0777) == -1) {
            if (errno != EEXIST) {
                fprintf(stderr, "mkdir[%s] failed with errno[%d]\n",
                        path, errno);
                return NULL;
            }
        }
        if (chmod(path, S_IRUSR | S_IWUSR | S_IXUSR
                  | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == -1)
        {
            fprintf(stderr, "chmod[%s] failed with errno[%d]\n", path, errno);
            return NULL;
        }

        strcat(path + n, fn);

        int fd = open(path, O_WRONLY | O_CREAT);
        if (fd == -1) {
            fprintf(stderr, "open[%s] for write failed with errno[%d]",
                    path, errno);
            return NULL;
        }

        replica_file2_t *f = (replica_file2_t *)malloc(sizeof(replica_file2_t));
        if (f == NULL) {
            fprintf(stderr, "malloc replica_file2_t for [%s] failed\n", path);
            close(fd);
            return NULL;
        }
        f->fd = fd;
        f->offset = 0;
        snprintf(f->fn, sizeof(f->fn), "%s", fn);
        g_file_map[dn] = f;

        return f;
    } else {
        if (strcmp(it->second->fn, fn) == 0) {
            if (offset != it->second->offset) {
                //[resp code: 4][dn: 64][fn: 32][offset: 8]
                char buf[109];
                uint32_t c = htonl(1);
                uint64_t o = htonll((uint64_t)it->second->offset);

                buf[108] = '\0';
                memcpy(buf, (char *)&c, 4);
                memcpy(buf + 4, dn, strlen(dn) + 1);
                memcpy(buf + 4 + 64, fn, strlen(fn) + 1);
                memcpy(buf + 4 + 64 + 32, (char *)&o, 8);
                if (sendall(sfd, buf, sizeof(buf), 0) != sizeof(buf)) {
                    fprintf(stderr, "sendall failed\n");
                    return NULL;
                }
            }
        } else {
            //FIXME
#define FILE_ROTAT_SIZE (1 * 1024 * 1024 * 1024)

            // file rotation size
            if (it->second->offset < FILE_ROTAT_SIZE) {
                //[resp code: 4][dn: 64][fn: 32][offset: 8]
                char buf[109];
                uint32_t c = htonl(2);
                uint64_t o = htonll((uint64_t)it->second->offset);

                buf[108] = '\0';
                memcpy(buf, (char *)&c, 4);
                memcpy(buf + 4, dn, strlen(dn) + 1);
                memcpy(buf + 4 + 64, fn, strlen(it->second->fn) + 1);
                memcpy(buf + 4 + 64 + 32, (char *)&o, 8);
                if (sendall(sfd, buf, sizeof(buf), 0) != sizeof(buf)) {
                    fprintf(stderr, "sendall failed\n");
                    return NULL;
                }
            }

            close(it->second->fd);
            it->second->fd = -1;
            snprintf(path, sizeof(path), "%s/%s/%s", g_root_dir, dn, fn);

            int fd = open(path, O_WRONLY | O_CREAT);
            if (fd == -1) {
                fprintf(stderr, "open[%s] failed with errno[%d]\n",
                        path, errno);
                return NULL;
            }
            it->second->fd = fd;
            snprintf(it->second->fn, sizeof(it->second->fn), "%s", fn);
            it->second->offset = 0;
        }

        return it->second;
    }

    return NULL;
}


static ssize_t do_write(replica_file2_t *f, const char *data, uint32_t mlen)
{
    if (f == NULL) return 0;

    off_t curr_offset = lseek(f->fd, f->offset, SEEK_SET);

    if (curr_offset == (off_t)-1) {
        fprintf(stderr, "lseek[%d][%s] failed with errno[%d]\n",
                f->fd, f->fn, errno);
        return -1;
    }

    ssize_t n = write(f->fd, data, mlen);

    if (n != mlen) {
        fprintf(stderr, "write[%d][%s] failed with errno[%d]\n",
                f->fd, f->fn, errno);
        return n;
    }

    f->offset = curr_offset + mlen;

    return n;
}


#define MAX_RECV_LEN 8192 // 8K


// 1) broker master alive & replica alive
// 2) broker master alive & replica leader crashed
// 3) replica follower crashed
static int handle_request_1(int sfd)
{
    char *buf = (char *)malloc(MAX_RECV_LEN);

    if (buf == NULL) {
        fprintf(stderr, "malloc recv buf failed for conn[%d]\n", sfd);
        return -1;
    }

    if (recvall(sfd, buf, 4, 0) != 4) {
        fprintf(stderr, "recvall[%d] the head 4 bytes failed\n", sfd);
        free(buf);
        return -1;
    }

    buf[4] = '\0';

    uint32_t l = ntohl((uint32_t)atol(buf));
    ssize_t n = recvall(sfd, buf, l - 4, 0);

    if (n != (ssize_t)(l - 4)) {
        fprintf(stderr, "recvall failed with expecting[%u] but getting[%ld]",
                l - 4, n);
        free(buf);
        return -1;
    }

    char *dn = buf, *fn = buf + 64, *data = buf + 64 + 32 + 8, saved = data[0];
    off_t offset = (data[0] = '\0', ntohll(strtoull(buf + 64 + 32, NULL, 10)));
    uint32_t mlen = l - 4 - 64 - 32 - 8;
    replica_file2_t *f = prepare_write(dn, fn, offset, sfd);

    data[0] = saved;

    n = do_write(f, data, mlen);
    if (n == 0) {
        // noop
    } else if (n != mlen) {
        fprintf(stderr, "do_write[%s/%s] failed\n", dn, fn);
        free(buf);
        return -1;
    }

    uint32_t resp_code = htonl(0);

    if (sendall(sfd, (char *)&resp_code, sizeof(uint32_t), 0)
        != sizeof(uint32_t))
    {
        free(buf);
        return -1;
    }

    free(buf);
    return 0;
}


static int stop_broker(int role, const char *broker_id)
{
    char cmd[64];

    switch (role) {
    case ROLE_LEADER:
        snprintf(cmd, sizeof(cmd), "%s-%s stop",
                "/etc/init.d/qbus-broker-master", broker_id);
        break;
    case ROLE_FOLLOWER:
        if (!g_broker_slave_running) return 0;
        snprintf(cmd, sizeof(cmd), "%s-%s stop",
                "/etc/init.d/qbus-broker-slave", broker_id);
        break;
    default:
        fprintf(stderr, "Invalid role\n");
        return -1;
    }

    FILE *pp = popen(cmd, "re");
    if (pp == NULL) {
        fprintf(stderr, "popen failed with errno[%d]\n", errno);
        return -1;
    }

    if (pclose(pp) == -1) {
        fprintf(stderr, "pclose failed with errno[%d]\n", errno);
        return -1;
    }

    if (role == ROLE_FOLLOWER) {
        g_broker_slave_running = false;
    }

    return 0;
}


static int handle_request_3(int sfd, replica_file2_t *f)
{
    return 0;
}


// reverse alpha sort
//static int fn_cmp(const struct dirent **p, const struct dirent **q)
static int fn_cmp(const void *p, const void *q)
{
    const struct dirent **p1 = (const struct dirent **)p,
                        **q1 = (const struct dirent **)q;
    if (strcmp((*p1)->d_name, (*q1)->d_name) < 0) return 1;
    else if (strcmp((*p1)->d_name, (*q1)->d_name) > 0) return -1;
    else return 0;
}


// 1) machine broken
// 2) broker master crashed & relica leader alive
static int handle_request_2(int sfd)
{
    if (g_broker_slave_running) {
        // XXX
        stop_broker(ROLE_FOLLOWER, g_broker_id);
    }

    char buf[4 + 64 + 32 + 8 + 1];

    if (recvall(sfd, buf, sizeof(buf) - 1, 0) != sizeof(buf) - 1) {
        fprintf(stderr, "recvall[%d] failed\n", sfd);
        return -1;
    }

    buf[4 + 64 + 32 + 8] = '\0';

    char *dn = buf + 4, *fn = dn + 64, path[256];
    off_t offset = (off_t)ntohll(strtoull(buf + 4 + 64 + 32, NULL, 10));
    struct dirent **namelist;
    int n = snprintf(path, sizeof(path), "%s/%s/", g_root_dir, dn),
        nentries = scandir(path, &namelist, NULL, fn_cmp);
    uint16_t nfiles;
    uint64_t totalen = 0;

    if (nentries == -1) {
        fprintf(stderr, "scandir[%s] failed with errno[%d]\n", path, errno);
        return -1;
    }

    off_t *lens = (off_t *)malloc(8 * sizeof(off_t)), fl;

    memset(lens, -1, sizeof(off_t) * 8);

    for (int i = 0; i < nentries; i++) {
        nfiles++;
        strcat(path + n, namelist[i]->d_name);
        fl = get_fsize_by_path(path);
        if (strcmp(namelist[i]->d_name, fn) == 0) {
            replica_file_map_t::iterator it = g_file_map.find(dn);
            if (it->second->offset < offset) {
                free(lens);
                free(namelist);
                return handle_request_3(sfd, it->second);
            }
            lens[i] = htonl(fl - offset);
            totalen += fl - offset;
            break;
        }
        lens[i] = htonl(fl);
        totalen += fl;
    }

    totalen += 8 + 4 + nfiles * (8 + 32);

    char *buf2 = (char *)malloc(totalen), *p;
    uint64_t temp = htonll(totalen);
    uint16_t temp2 = htons(nfiles);

    memcpy(buf2, (char *)&temp, sizeof(uint64_t));
    memcpy(buf2 + sizeof(uint64_t), (char *)&temp2, sizeof(uint16_t));

    p = buf2 + sizeof(uint64_t) + sizeof(uint16_t);
    for (int i = nfiles - 1; i >= 0; i--) {
        memcpy(p + (nfiles - 1 - i) * sizeof(off_t),
               (char *)&lens[i], sizeof(off_t));
    }
    p += nfiles * sizeof(off_t);
    for (int i = nfiles - 1; i >= 0; i--) {
        memcpy(p + (nfiles - 1 - i) * 32, namelist[i]->d_name, 32);
    }

    int ret = 0;

    if (sendall(sfd, buf2, totalen, 0) != (ssize_t)totalen) {
        fprintf(stderr, "sendall failed\n");
        ret = -1;
        goto rtn;
    }

rtn:
    free(buf2);
    free(lens);
    free(namelist);
    return ret;
}


int handle_leader_request(int sfd)
{
    if (g_broker_slave_running) {
        return handle_request_2(sfd);
    }

    return handle_request_1(sfd);
}
