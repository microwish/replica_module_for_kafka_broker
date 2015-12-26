#ifndef _SIMPLECONF_H
#define _SIMPLECONF_H


#include "SimpleHashtab.h"


Hashtab *load_conf(const char *conf_file);
void destroy_conf(Hashtab *conf);
char *read_conf(Hashtab *conf, const char *key);


#endif
