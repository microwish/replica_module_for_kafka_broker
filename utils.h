#ifndef _UTILS_H
#define _UTILS_H


#include "SimpleConnPool.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "thr_pool.h"

#ifdef __cplusplus
}
#endif


#include <string>
#include <map>

#include <zookeeper/zookeeper.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <errno.h>
#include <signal.h>
#include <sys/inotify.h>


typedef struct {
    uint32_t len;
    char data[];
} replica_sync_data_t;

typedef struct {
    int fd;
    off_t offset;
    char fn[32];
    uint32_t dnl;
    char dn[];
} replica_file_t;

typedef struct {
    conn_pool_t *cp;
    replica_sync_data_t *d;
    replica_file_t *f;
    uint32_t noff; // offset to be added
} replica_sync_t;

typedef struct {
    uint32_t len;
    char buf[];
} file_tail_t;

typedef struct {
    int fd;
    off_t offset;
    char fn[32];
} replica_file2_t;

// inotify watch descriptor -->
typedef std::map<int, replica_file_t *> replica_inot_map_t;

// file relative path to g_root_dir -->
typedef std::map<std::string, replica_file2_t *> replica_file_map_t;


// role of replica
enum {
    ROLE_LEADER = 0,
    ROLE_FOLLOWER
};


#define set_nonblocking(s) fcntl(s, F_SETFL, fcntl(s, F_GETFL) | O_NONBLOCK)


extern int g_ep_wait_timeout, g_follower_primary_port,
       g_follower_secondary_port, g_recv_timeout, g_send_timeout,
       g_broker_alive;
extern bool g_leader_on, g_follower_on, g_broker_slave_running;
extern char *g_follower_host, *g_zk_hosts, *g_data_dirs, *g_broker_id,
       *g_root_dir;
extern thr_pool_t *g_tpool;
extern conn_pool_t *g_cpool;
extern replica_inot_map_t g_inot_map;
extern zhandle_t *g_zh;


bool init_config(const char *conf_file);
int init_sig_handlers(int sig);
bool build_topic_dir_map();
int init_socket(short int port, int backlog);
int init_watches(int inotify_fd, replica_inot_map_t& rmap);
bool handle_events(int inotify_fd);
void do_watch(zhandle_t *zh, int type, int state, const char *path, void *wctx);
ssize_t recvall(int sockfd, void *buf, size_t len, int flags);
ssize_t sendall(int sockfd, const void *buf, size_t len, int flags);
int handle_leader_request(int sfd);
int start_broker(int role, const char *broker_id);
int prefill_conn_pool(conn_pool_t *cp, int num);


#endif
