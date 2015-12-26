#include "rleader.h"
#include "rfollower.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


#define DEFAULT_CONF_FILE "/home/infra/kafka/replica/config/replica.conf"


int main(int argc, char *argv[])
{
    int opt;
    const char *conf_file = NULL;

    if (argc < 2) {
        fprintf(stderr, "Usage: /home/infra/kafak/replica/bin/replica -c\n");
        exit(EXIT_FAILURE);
    }

    while ((opt = getopt(argc, argv, "c:")) != -1) {
        switch (opt) {
        case 'c':
            conf_file = optarg;
            break;
        }
    }

    if (conf_file == NULL) conf_file = DEFAULT_CONF_FILE;

    if (!init_config(conf_file)) {
        fprintf(stderr, "init_config[%s] failed\n", conf_file);
        exit(EXIT_FAILURE);
    }

    if (g_follower_on) {
        if (follower_main() == -1) {
            exit(EXIT_FAILURE);
        }
    }

    if (g_leader_on) {
        if (leader_main() == -1) {
            exit(EXIT_FAILURE);
        }
    }

    exit(EXIT_SUCCESS);
}
