#include "rfollower.h"

#include <sys/epoll.h>
#include <errno.h>


#define MAX_EPOLL_EVENTS 512

int follower_main()
{
    char zkpath[24];

    if ((g_zh = zookeeper_init(g_zk_hosts, do_watch, 5, NULL, g_zk_hosts, 0))
        == NULL)
    {
        fprintf(stderr, "zookeeper_init[%s] failed with errno[%d]\n",
                g_zk_hosts, errno);
        return -1;
    }

    snprintf(zkpath, sizeof(zkpath), "/brokers/ids/%s", g_broker_id);

    int rc = zoo_exists(g_zh, zkpath, 1, NULL);
    switch (rc) {
    case ZOK:
        g_broker_alive = 1;
        break;
    case ZNONODE:
        fprintf(stderr, "No alive broker[%s]\n", zkpath);
        g_broker_alive = 0;
        break;
    default:
        fprintf(stderr, "zoo_exists[%s] failed with err[%d][%s]\n",
                zkpath, rc, zerror(rc));
        zookeeper_close(g_zh);
        return -1;
    }

    if (!build_topic_dir_map()) {
        fprintf(stderr, "build_topic_dir_map failed\n");
        zookeeper_close(g_zh);
        return -1;
    }

    int listen_sfd = init_socket(g_follower_secondary_port, 256);
    if (listen_sfd == -1) {
        fprintf(stderr, "init_socket for primary port[%d] failed\n",
                g_follower_primary_port);
        zookeeper_close(g_zh);
        return -1;
    }

    int listen_sfd2 = init_socket(g_follower_secondary_port, 4);
    if (listen_sfd2 == -1) {
        fprintf(stderr, "init_socket for secondary port[%d] failed\n",
                g_follower_secondary_port);
        close(listen_sfd);
        zookeeper_close(g_zh);
        return -1;
    }

    g_broker_slave_running = false;

    int epfd = epoll_create(512);
    if (epfd == -1) {
        fprintf(stderr, "epoll_create failed with errno[%d]\n", errno);
        close(listen_sfd2);
        close(listen_sfd);
        zookeeper_close(g_zh);
        return -1;
    }

    struct epoll_event events[MAX_EPOLL_EVENTS], ev;
    int nfds, ret = 0;
    struct sockaddr_in cli_addr;
    socklen_t al;

    ev.events = EPOLLIN;
    ev.data.fd = listen_sfd;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_sfd, &ev) == -1) {
        fprintf(stderr, "epoll_ctl EPOLL_CTL_ADD failed with errno[%d]\n",
                errno);
        ret = -1;
        goto rtn;
    }

    ev.events = EPOLLIN;
    ev.data.fd = listen_sfd2;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_sfd2, &ev) == -1) {
        fprintf(stderr, "epoll_ctl EPOLL_CTL_ADD failed with errno[%d]\n",
                errno);
        ret = -1;
        goto rtn;
    }

    for (;;) {
        if ((nfds = epoll_wait(epfd, events, MAX_EPOLL_EVENTS, g_ep_wait_timeout))
            == -1)
        {
            // XXX
            // e.g. signal interruption
            fprintf(stderr, "epoll_wait failed with errno[%d]\n", errno);
            sleep(1);
            continue;
        } else if (nfds == 0) {
            if (g_broker_alive == 1) {
                continue;
            }
            rc = zoo_exists(g_zh, zkpath, 1, NULL);
            switch (rc) {
            case ZOK:
                g_broker_alive = 1;
                continue;
            case ZNONODE:
                break;
            default:
                fprintf(stderr, "zoo_exists[%s] failed with err[%d][%s]\n",
                        zkpath, rc, zerror(rc));
                continue;
            }

            if (start_broker(ROLE_FOLLOWER, g_broker_id) == -1) {
                fprintf(stderr, "start_broker[%s] slave failed\n", g_broker_id);
                ret = -1;
                goto rtn;
            }
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == listen_sfd) {
                int conn_sfd = accept(listen_sfd,
                                      (struct sockaddr *)&cli_addr, &al);
                if (conn_sfd == -1) {
                    fprintf(stderr, "accept failed with errno[%d]\n", errno);
                    continue;
                }

                // FIXME
                // immediate data

                if (set_nonblocking(conn_sfd) == -1) {
                    fprintf(stderr, "Setting socket nonblocking failed with errno[%d]\n",
                            errno);
                    // XXX
                    continue;
                }
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = conn_sfd;
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, conn_sfd, &ev) == -1) {
                    fprintf(stderr, "epoll_ctl EPOLL_CTL_ADD "
                            "failed with errno[%d]\n", errno);
                    continue;
                }
            } else if (events[i].data.fd == listen_sfd2) {
                int conn_sfd = accept(listen_sfd2,
                                      (struct sockaddr *)&cli_addr, &al);
                if (conn_sfd == -1) {
                    fprintf(stderr, "accept failed with errno[%d]\n", errno);
                    ret = -1;
                    goto rtn;
                }

                char buf[5];

                if (recvall(conn_sfd, buf, 4, 0) != 4) {
                    fprintf(stderr, "recvall failed on socket[%d]\n",
                            listen_sfd2);
                    ret = -1;
                    goto rtn;
                }

                uint32_t req_code, resp_code;

                buf[4] = '\0';
                req_code = ntohl(atoi(buf));

                // TODO
                // set up resp_code
                switch (req_code) {
                default:
                    ret = -1;
                    goto rtn;
                }

                if (sendall(conn_sfd, (char *)&resp_code, sizeof(int), 0)
                    != sizeof(int))
                {
                    fprintf(stderr, "sendall failed on socket[%d]\n",
                            listen_sfd2);
                    ret = -1;
                    goto rtn;
                }

                close(conn_sfd);
            } else {
                int code = handle_leader_request(events[i].data.fd);
                sendall(events[i].data.fd, (char *)&code, sizeof(int), 0);
                continue;
            }
        }
    }

rtn:
    close(epfd);
    close(listen_sfd2);
    close(listen_sfd);
    zookeeper_close(g_zh);
    return ret;
}
