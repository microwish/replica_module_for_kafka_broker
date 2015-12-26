#include "HSearchHelper.h"

#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>


#ifdef __cplusplus
extern "C" {
#endif


#define __set_errno(val) (errno = (val))

#define HTAB_SIZE ((htab)->size + 1)


int hrelease_r(struct hsearch_data *htab, int manual)
{
    unsigned int i;

    if (htab->filled == 0) return 1;

    for (i = 0; i < HTAB_SIZE; i++) {
        switch (htab->table[i].used) {
        case (unsigned int)-1:
            htab->table[i].used = 0;
        case 0:
            break;
        default:
            htab->table[i].used = 0;
            if (manual & 0x01) free(htab->table[i].entry.key);
            else if (manual & 0x02) delete htab->table[i].entry.key;
            if (manual & 0x04) free(htab->table[i].entry.data);
            // FIXME
            //else if (manual & 0x08) delete htab->table[i].entry.data;
            htab->table[i].entry.key = NULL;
            htab->table[i].entry.data = NULL;
            break;
        }
    }

    htab->filled = 0;

    return 1;
}


// modified from glibc-2.19 misc/hsearch_r.c
int hsearch2_r(ENTRY item, ACTION2 action,
               ENTRY **retval, struct hsearch_data *htab, int manual)
{
  unsigned int hval;
  unsigned int count;
  unsigned int len = strlen (item.key);
  unsigned int idx;

  /* Compute an value for the given string. Perhaps use a better method. */
  hval = len;
  count = len;
  while (count-- > 0)
    {
      hval <<= 4;
      hval += item.key[count];
    }
  if (hval == 0)
    ++hval;

  /* First hash function: simply take the modul but prevent zero. */
  idx = hval % htab->size + 1;

  // n.b. open-address (re-hash) to deal with collision
  // used == (unsigned int)-1 means
  // this slot was ever released and could be reused now
  if (htab->table[idx].used)
    {
      /* Further action might be required according to the action value. */
      if (htab->table[idx].used == hval
          && htab->table[idx].entry.key != NULL
	  && strcmp (item.key, htab->table[idx].entry.key) == 0)
	{
        switch (action) {
        case REPLACE:
            if (manual & 0x04) free(htab->table[idx].entry.data);
            // FIXME
            //else if (manual & 0x08) delete htab->table[idx].entry.data;
            htab->table[idx].entry.data = item.data;
            break;
        case RELEASE:
            --htab->filled;
            if (manual & 0x01) free(htab->table[idx].entry.key);
            else if (manual & 0x02) delete htab->table[idx].entry.key;
            if (manual & 0x04) free(htab->table[idx].entry.data);
            // FIXME
            //else if (manual & 0x08) delete htab->table[idx].entry.data;
            htab->table[idx].entry.key = NULL;
            htab->table[idx].used = (unsigned int)-1;
            break;
        default:
            break;
        }

	  *retval = &htab->table[idx].entry;
	  return 1;
	} else if (htab->table[idx].used == (unsigned int)-1) {
        if (action == ENTER2) {
            htab->table[idx].used  = hval;
            htab->table[idx].entry = item;

            ++htab->filled;

            *retval = &htab->table[idx].entry;
            return 1;
        }
    }

      /* Second hash function, as suggested in [Knuth] */
      unsigned int hval2 = 1 + hval % (htab->size - 2);
      unsigned int first_idx = idx;

      do
	{
	  /* Because SIZE is prime this guarantees to step through all
             available indeces.  */
          if (idx <= hval2)
	    idx = htab->size + idx - hval2;
	  else
	    idx -= hval2;

	  /* If we visited all entries leave the loop unsuccessfully.  */
	  if (idx == first_idx)
	    break;

            /* If entry is found use it. */
          if (htab->table[idx].used == hval
              && htab->table[idx].entry.key != NULL
	      && strcmp (item.key, htab->table[idx].entry.key) == 0)
	    {
            switch (action) {
            case REPLACE:
                if (manual & 0x04) free(htab->table[idx].entry.data);
                // FIXME
                //else if (manual & 0x08) delete htab->table[idx].entry.data;
	        htab->table[idx].entry.data = item.data;
                break;
            case RELEASE:
                --htab->filled;
                if (manual & 0x01) free(htab->table[idx].entry.key);
                else if (manual & 0x02) delete htab->table[idx].entry.key;
                if (manual & 0x04) free(htab->table[idx].entry.data);
                // FIXME
                //else if (manual & 0x08) delete htab->table[idx].entry.data;
                htab->table[idx].entry.key = NULL;
                htab->table[idx].used = (unsigned int)-1;
                break;
            default:
                break;
            }

	      *retval = &htab->table[idx].entry;
	      return 1;
	    } else if (htab->table[idx].used == (unsigned int)-1) {
            if (action == ENTER2) {
                htab->table[idx].used  = hval;
                htab->table[idx].entry = item;

                ++htab->filled;

                *retval = &htab->table[idx].entry;
                return 1;
            }
        }
	}
      while (htab->table[idx].used);
    }

  /* An empty bucket has been found. */
  if (action == ENTER2)
    {
      /* If table is full and another entry should be entered return
	 with error.  */
      if (htab->filled == htab->size)
	{
	  __set_errno (ENOMEM);
	  *retval = NULL;
	  return 0;
	}

      htab->table[idx].used  = hval;
      htab->table[idx].entry = item;

      ++htab->filled;

      *retval = &htab->table[idx].entry;
      return 1;
    }

  __set_errno (ESRCH);
  *retval = NULL;
  return 0;
}


#ifdef __cplusplus
}
#endif
