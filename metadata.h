/* 2021 Nov 13 by Cao
   Introducing system table metadata struct */

#pragma once
#ifndef _METADATA_H_
#define _METADATA_H_

typedef struct TableRec {
	char tablename[21];
	int  attrcount;
} TableRec;

typedef struct ColumnRec {
	char tablename[21];
	char attrname[21];
	int  attrtype;
	int  attrlength;
	int  attroffset;
	bool ix_flag;
	char indexname[21];
} ColumnRec;

#endif
