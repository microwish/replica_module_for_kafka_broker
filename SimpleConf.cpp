#include "SimpleConf.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>


static int clear_conf(const char *key, void *data)
{
    (void)key;
    free(data);
    return 0;
}


Hashtab *load_conf(const char *conf_file)
{
    if (conf_file == NULL || conf_file[0] == '\0') {
        fprintf(stderr, "Invalid conf file path\n");
        return NULL;
    }

    FILE *fp = fopen(conf_file, "r");

    if (fp == NULL) {
        fprintf(stderr, "fopen[%s] failed with errno[%d]\n",
                conf_file, errno);
        return NULL;
    }

    Hashtab *conf = new Hashtab(37);

    if (conf->getErrno() != 0) {
        delete conf;
        if (fclose(fp) != 0) {
            fprintf(stderr, "fclose[%s] failed with errno[%d]\n",
                    conf_file, errno);
        }
        return NULL;
    }

    char line[256];
    int ks, ke, vs, ve;

    while (feof(fp) == 0) {
        char *val;
        int i = 0;

        if (fgets(line, sizeof(line), fp) == NULL) {
            fprintf(stderr, "fgets[%s] failed\n", conf_file);
            goto err_rtn;
        }

        if (line[0] == '#') continue;

        while (!isalnum(line[i])) i++;
        ks = i;
        do { i++; } while (isalnum(line[i]));
        ke = i;

        while (line[i] != '=') i++;
        do { i++; } while (isspace(line[i]));
        vs = i;
        while (line[i] != '\n' && line[i] != '\0') i++;
        if (line[i] == '\n') line[i] = '\0';
        ve = i;

        if ((val = (char *)malloc(ve - vs + 1)) == NULL) {
            fprintf(stderr, "malloc[%s] failed\n", line);
            goto err_rtn;
        }

        memcpy(val, line + vs, ve - vs + 1);

        line[ke] = '\0';

        if (!conf->insert(line, val)) {
            free(val);
            goto err_rtn;
        }
    }

    if (fclose(fp) != 0) {
        fprintf(stderr, "fclose[%s] failed with errno[%d]\n",
                conf_file, errno);
    }

    return conf;

err_rtn:
    if (conf != NULL) {
        conf->traverse(clear_conf, 0);
        delete conf;
    }
    if (fclose(fp) != 0) {
        fprintf(stderr, "fclose[%s] failed with errno[%d]\n",
                conf_file, errno);
    }

    return NULL;
}


void destroy_conf(Hashtab *conf)
{
    if (conf != NULL) {
        conf->traverse(clear_conf, 0);
        delete conf;
        conf = NULL;
    }
}


char *read_conf(Hashtab *conf, const char *key)
{
    if (conf == NULL) return NULL;

    void *data = conf->findData(key);

    return data == NULL ? NULL : (char *)data;
}
