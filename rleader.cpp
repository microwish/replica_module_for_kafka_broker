#include "rleader.h"

#include <signal.h>


#define MAX_THREADS 64
#define MIN_THREADS 16
#define MAX_CONNS MAX_THREADS
#define MIN_CONNS MIN_THREADS


int leader_main()
{
    if (init_sig_handlers(SIGUSR2) == -1) {
        fprintf(stderr, "init_sig_handlers failed\n");
        return -1;
    }

    g_tpool = thr_pool_create(MIN_THREADS, MAX_THREADS, 60, NULL);
    if (g_tpool == NULL) {
        fprintf(stderr, "thr_pool_create failed with errno[%d]\n", errno);
        return -1;
    }

    g_cpool = conn_pool_create(MAX_CONNS);
    if (g_cpool == NULL) {
        fprintf(stderr, "conn_pool_create failed\n");
        thr_pool_destroy(g_tpool);
        return -1;
    }

    if (prefill_conn_pool(g_cpool, MIN_CONNS) == 0) {
        fprintf(stderr, "prefill_conn_pool failed\n");
        conn_pool_destroy(g_cpool);
        thr_pool_destroy(g_tpool);
        return -1;
    }

    // XXX
    //int inot_fd = inotify_init1(IN_NONBLOCK);
    int inot_fd = inotify_init();
    if (inot_fd == -1) {
        fprintf(stderr, "inotify_init failed with errno[%d]\n", errno);
        conn_pool_destroy(g_cpool);
        thr_pool_destroy(g_tpool);
        return -1;
    }

    int n = init_watches(inot_fd, g_inot_map), ret = -1;

    if (n == -1) {
        fprintf(stderr, "Initializing inotify watch for [%s] failed\n",
                g_root_dir);
        goto rtn;
    } else if (n == 0) {
        fprintf(stderr, "No directories to be watched for [%s]\n",
                g_root_dir);
        goto rtn;
    }

    if (handle_events(inot_fd)) {
        ret = 0;
    }

rtn:
    if (g_inot_map.size() > 0) {
        replica_inot_map_t::iterator it = g_inot_map.begin();
        for (; it != g_inot_map.end(); it++) {
            inotify_rm_watch(inot_fd, it->first);
            free(it->second);
        }
    }
    if (close(inot_fd) == -1) {
        fprintf(stderr, "close inotify fd failed with errno[%d]\n", errno);
    }
    conn_pool_destroy(g_cpool);
    thr_pool_destroy(g_tpool);
    return ret;
}
