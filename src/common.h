#ifndef COMMON_CHECK_H
#define COMMON_CHECK_H

#include "postgres.h"
#include "access/heapam.h"

uint32 check_page_header(PageHeader header, BlockNumber block);

#endif
