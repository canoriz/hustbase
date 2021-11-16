#ifndef PF_MANAGER_H_H
#define PF_MANAGER_H_H

#include <stdio.h>
#include <io.h>
#include <fcntl.h>
#include <sys/types.h>

#include <sys/stat.h>
#include <string.h>
#include <time.h>
#include <malloc.h>

#include "RC.h"

#define PF_PAGE_SIZE ((1<<12)-4)
#define PF_FILESUBHDR_SIZE (sizeof(PF_FileSubHeader))
#define PF_BUFFER_SIZE 50//页面缓冲区的大小
#define PF_PAGESIZE (1<<12)
#define MAX_OPEN_FILE 50

typedef unsigned int PageNum;

typedef struct {
	PageNum pageNum;				//页号(由0开始编号)
	char pData[PF_PAGE_SIZE];		//数据区
}Page;

typedef struct {
	PageNum pageCount;			//尾页的页号
	int nAllocatedPages;			//已分配页的数目(包括控制页)
}PF_FileSubHeader;

typedef struct {
	bool bDirty;		//True 表示页面内容已经被修改，False表示没有被修改
	unsigned int pinCount; 	//访问当前页面的线程数
	time_t 	accTime;		//页面最后访问时间
	char* fileName;			//访问页面所在的文件名
	int fileDesc;				//与该文件相关联的文件描述符
	Page page;				//从硬盘中获取的实际页面
}Frame;

typedef struct {
	bool bOpen;			//该文件句柄是否已经与一个文件关联
	char* fileName;			//与该句柄关联的文件名
	int  fileDesc;			//与该句柄关联的文件描述符
	Frame* pHdrFrame;		//指向该文件头帧（控制页对应的帧）的指针
	Page* pHdrPage;		//指向该文件头页（控制页）的指针
	char* pBitmap;		//指向控制页中位图的指针
	PF_FileSubHeader *pFileSubHeader;	//该文件的PF_FileSubHeader结构
} PF_FileHandle;
	

typedef struct __BF_Manager {
	int nReads;
	int nWrites;
	Frame frame[PF_BUFFER_SIZE];
	bool allocated[PF_BUFFER_SIZE];

	__BF_Manager()
	{
		for (int i = 0; i < PF_BUFFER_SIZE; i++) {
			allocated[i] = false;
			frame[i].pinCount = 0;
		}
	}

}BF_Manager;

typedef struct {
	bool  bOpen;			//True 表示页面句柄被打开
	Frame* pFrame;			//指向当前对象句柄的指针
}PF_PageHandle;

//创建一个名称为指定文件名的分页文件。
const RC CreateFile(const char *fileName);

//根据文件名打开一个分页文件，返回文件ID。
const RC OpenFile(char *fileName,int * FileID);

//关闭FileID对应的分页文件。
const RC CloseFile(int FileID);

//根据文件ID和页号获取指定页面到缓冲区，返回页面句柄指针。
const RC GetThisPage(int FileID,PageNum pageNum,PF_PageHandle *pageHandle);

//在指定文件中分配一个新的页面，并将其放入缓冲区，返回页面句柄指针。分配页面时，如果文件中有空闲页，就直接分配一个空闲页；如果文件中没有空闲页，则扩展文件规模来增加新的空闲页。
const RC AllocatePage(int FileID,PF_PageHandle *pageHandle);

//根据页面句柄指针返回对应的页面号。
const RC GetPageNum(PF_PageHandle *pageHandle,PageNum *pageNum);
const RC GetFileHandle(PF_FileHandle** fileHandle, int fileID);
const RC GetData(PF_PageHandle *pageHandle,char **pData);

//丢弃文件中编号为pageNum的页面，将其变为空闲页。
const RC DisposePage(int FileID,PageNum pageNum);

//标记指定页面为“脏”页。如果修改了页面的内容，则应调用此函数，以便该页面被淘汰出缓冲区时系统将新的页面数据写入磁盘文件。
const RC MarkDirty(PF_PageHandle *pageHandle);

//此函数用于解除pageHandle对应页面的驻留缓冲区限制。在调用GetThisPage或AllocatePage函数将一个页面读入缓冲区后，该页面被设置为驻留缓冲区状态，以防止其在处理过程中被置换出去，因此在该页面使用完之后应调用此函数解除该限制，使得该页面此后可以正常地被淘汰出缓冲区。
const RC UnpinPage(PF_PageHandle *pageHandle);

//获取文件的总页数。
const RC GetPageCount(int FileID, int *pageCount);

#endif
