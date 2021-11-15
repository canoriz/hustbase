#include "RM_Manager.h"
#include "str.h"
#include <math.h>



//辅助函数，获取下一条记录的位置，条件：rmFileScan已经打开
RC nextRec(RM_FileScan* rmFileScan)
{
	if (rmFileScan->pn == -1 && rmFileScan->sn == -1)
		return FAIL;
	int fileID = rmFileScan->pRMFileHandle->fileDesc;

	PF_FileHandle* fileHandle;
	PF_PageHandle pageHandle;
	RC temp = GetFileHandle(fileHandle, fileID);
	if (temp != SUCCESS)
		return temp;
	int page, slot;

	for (page = rmFileScan->pn; page < fileHandle->pFileSubHeader->pageCount; ++page) {
		int bitmap = 1 << (page % 8);//位图中位置
		bool isEmpty = fileHandle->pBitmap[page / 8] & bitmap;
		if (!isEmpty)
			continue;
		GetThisPage(fileID, page, &pageHandle);
		slot = (page == rmFileScan->pn ? rmFileScan->sn + 1 : 0);
		//找到第一条记录
		while (slot < rmFileScan->pRMFileHandle->fileSubHeader->recordsPerPage && (pageHandle.pFrame->page.pData[slot / 8] & 1 << (slot % 8)) == 0) {
			slot++;
		}
		UnpinPage(&pageHandle);
		//若找到下一条记录 则跳出
		if (slot < rmFileScan->pRMFileHandle->fileSubHeader->recordsPerPage)
			break;
	}

	//未找到
	if (page >= fileHandle->pFileSubHeader->pageCount) {
		rmFileScan->pn = -1;
		rmFileScan->sn = -1;
		rmFileScan->PageHandle.bOpen = false;
		rmFileScan->PageHandle.pFrame = nullptr;
		return RM_EOF;
	}
	else {
		rmFileScan->PageHandle = pageHandle;
		rmFileScan->pn = page;
		rmFileScan->sn = slot;
	}
	return SUCCESS;
}

/*
打开一个文件扫描。本函数利用从第二个参数开始的所有输入参数初始化一个由参数rmFileScan指向的文件扫描结构，在使用中，
用户应先调用此函数初始化文件扫描结构，然后再调用GetNextRec函数来逐个返回文件中满足条件的记录。如果条件数量conNum为0，
则意味着检索文件中的所有记录。如果条件不为空，则要对每条记录进行条件比较，只有满足所有条件的记录才被返回。
*/
RC OpenScan(RM_FileScan *rmFileScan, RM_FileHandle *fileHandle, int conNum, Con *conditions)//³õÊ¼»¯É¨Ãè
{	
	rmFileScan->bOpen = true;
	rmFileScan->pRMFileHandle = fileHandle;
	rmFileScan->conditions = conditions;
	rmFileScan->conNum = conNum;
	rmFileScan->PageHandle.bOpen = false;
	rmFileScan->PageHandle.pFrame = nullptr;
	rmFileScan->pn = 2;
	rmFileScan->sn = 0;
	return SUCCESS;
}

RC CloseScan(RM_FileScan* rmFileScan) {
	rmFileScan->bOpen = false;
	rmFileScan->pn = -1;
	rmFileScan->sn = -1;
	rmFileScan->conditions = nullptr;
	rmFileScan->PageHandle.bOpen = false;
	rmFileScan->PageHandle.pFrame = nullptr;
	return SUCCESS;
}

/*
获取一个符合扫描条件的记录。如果该方法成功，返回值rec应包含记录副本及记录标识符。如果没有发现满足扫描条件的记录，则返回RM_EOF。
*/
RC GetNextRec(RM_FileScan *rmFileScan, RM_Record *rec)
{
	int firstRecordOffset;
	int recordSize;

	firstRecordOffset = rmFileScan->pRMFileHandle->fileSubHeader->firstRecordOffset;//第一条偏移
	recordSize = rmFileScan->pRMFileHandle->fileSubHeader->recordSize;//每个记录大小 

	//pData = 初始地址 + 第一条偏移 + 记录大小 × sn（当前扫描记录）
	char* pData = nullptr;
	RC temp;
	while ((temp = nextRec(rmFileScan)) == SUCCESS) {	// 找到了一条记录

		GetThisPage(rmFileScan->pRMFileHandle->fileDesc, rmFileScan->pn, &rmFileScan->PageHandle);
		pData = rmFileScan->PageHandle.pFrame->page.pData + firstRecordOffset + rmFileScan->sn * recordSize;

		int i;
		for (i = 0; i < rmFileScan->conNum; i++){
			Con* pCon = rmFileScan->conditions + i;
			bool isMatch = false;
			switch (pCon->attrType){
			case ints:
				int l = pCon->bLhsIsAttr == 1 ? *(int*)pData + pCon->LattrOffset : *(int*)pCon->Lvalue;
				int r = pCon->bRhsIsAttr == 1 ? *(int*)pData + pCon->RattrOffset : *(int*)pCon->Rvalue;
				switch (pCon->compOp){
				case EQual:
					isMatch = (l == r);
					break;
				case LessT:
					isMatch = (l < r);
					break;
				case GreatT:
					isMatch = (l > r);
					break;
				case NEqual:
					isMatch = (l != r);
					break;
				case LEqual:
					isMatch = (l <= r);
					break;
				case GEqual:
					isMatch = (l >= r);
					break;
				case NO_OP:
					break;
				default:
					break;
				}
				break;
			case floats:
				float l = pCon->bLhsIsAttr == 1 ? *(float*)pData + pCon->LattrOffset : *(float*)pCon->Lvalue;
				float r = pCon->bRhsIsAttr == 1 ? *(float*)pData + pCon->RattrOffset : *(float*)pCon->Rvalue;
				switch (pCon->compOp) {
				case EQual:
					isMatch = (l == r);
					break;
				case LessT:
					isMatch = (l < r);
					break;
				case GreatT:
					isMatch = (l > r);
					break;
				case NEqual:
					isMatch = (l != r);
					break;
				case LEqual:
					isMatch = (l <= r);
					break;
				case GEqual:
					isMatch = (l >= r);
					break;
				case NO_OP:
					break;
				default:
					break;
				}
				break;
			case chars:
				char* lValue = (pCon->bLhsIsAttr == 1 ? pData + pCon->LattrOffset : (char *)pCon->Lvalue);
				char* rValue = (pCon->bRhsIsAttr == 1 ? pData + pCon->RattrOffset : (char*)pCon->Rvalue);
				switch (pCon->compOp) {
				case EQual:
					isMatch = (strcmp(lValue, rValue) == 0);
					break;
				case LessT:
					isMatch = (strcmp(lValue, rValue) < 0);
					break;
				case GreatT:
					isMatch = (strcmp(lValue, rValue) > 0);
					break;
				case NEqual:
					isMatch = (strcmp(lValue, rValue) != 0);
					break;
				case LEqual:
					isMatch = (strcmp(lValue, rValue) <= 0);
					break;
				case GEqual:
					isMatch = (strcmp(lValue, rValue) >= 0);
					break;
				case NO_OP:
					break;
				default:
					break;
				}
				break;
			default:
				break;
			}
			if (!isMatch){
				UnpinPage(&rmFileScan->PageHandle);
				break;
			}
		}
		if (i >= rmFileScan->conNum)//全部匹配
			break;
	}

	if (temp != SUCCESS)
		return temp;

	//查找成功 修改rec
	rec->bValid = true;
	rec->pData = pData;
	rec->rid.bValid = true;
	rec->rid.pageNum = rmFileScan->pn;
	rec->rid.slotNum = rmFileScan->sn;
	UnpinPage(&rmFileScan->PageHandle);

	return SUCCESS;
}


/*
获取指定文件中标识符为rid的记录内容到rec指向的记录结构中。
*/
RC GetRec(RM_FileHandle *fileHandle, RID *rid, RM_Record *rec){
	int fileID = fileHandle->fileDesc;
	PF_FileHandle* pFileHandle;
	PF_PageHandle pageHandle;
	RC temp = GetFileHandle(pFileHandle, fileID);
	if (temp != SUCCESS)
		return temp;
	int page = rid->pageNum, slot = rid->slotNum;
	temp = GetThisPage(fileID, page, &pageHandle);
	if (temp != SUCCESS)
		return temp;
	//rid记录page、slot不符合
	if (rid->bValid == false || page <= 1 || page > pFileHandle->pFileSubHeader->pageCount || slot > fileHandle->fileSubHeader->recordsPerPage)
		return RM_INVALIDRID;
	//rid页为空或记录为空
	if ((pFileHandle->pBitmap[page / 8] & 1 << (page % 8)) == 0 || (pageHandle.pFrame->page.pData[slot / 8] & 1 << (slot % 8)) == 0)
		return RM_INVALIDRID;

	rec->rid = *rid;
	rec->bValid = true;
	rec->rid.bValid = true;
	int offset = fileHandle->fileSubHeader->firstRecordOffset;
	int size = fileHandle->fileSubHeader->recordSize;
	int pos = offset + size * slot;
	rec->pData = pageHandle.pFrame->page.pData + pos;
	UnpinPage(&pageHandle);
	return SUCCESS;
}

/*
插入一个新的记录到指定文件中，pData为指向新纪录内容的指针，返回该记录的标识符rid。
*/
RC InsertRec(RM_FileHandle *fileHandle, char *pData, RID *rid){
	int fileID = fileHandle->fileDesc;
	PF_FileHandle* pFileHandle;
	PF_PageHandle pageHandle;
	RC temp = GetFileHandle(pFileHandle, fileID);
	if (temp != SUCCESS)
		return temp;
	int pageCount;
	temp = GetPageCount(fileID, &pageCount);
	if (temp != SUCCESS)
		return temp;

	int page, slot;
	//找到可用的页
	for (page = 0; page <= pageCount; ++page) {
		if ((pFileHandle->pBitmap[page / 8] & 1 << (page % 8)) != 0 && (fileHandle->pBitmap[page / 8] & 1 << (page % 8)) == 0) {//已分配但未满的页
			break;
		}
	}

	if (page > pageCount) {//不存在可用页时，再分配
		temp = AllocatePage(fileID, &pageHandle);
		if (temp != SUCCESS)
			return temp;
		int offset = fileHandle->fileSubHeader->firstRecordOffset;
		int size = fileHandle->fileSubHeader->recordSize;
		int slotNum = fileHandle->fileSubHeader->recordsPerPage;

		memcpy(pageHandle.pFrame->page.pData + offset, pData, size);
		pageHandle.pFrame->page.pData[0] |= 1;

		if (slotNum == 1) {
			page = pageHandle.pFrame->page.pageNum;
			fileHandle->pBitmap[page / 8] |= (1 << (page % 8));
		}
		MarkDirty(&pageHandle);
		UnpinPage(&pageHandle);
		rid->bValid = true;
		rid->pageNum = pageHandle.pFrame->page.pageNum;
		rid->slotNum = 0;
	}
	else {
		temp = GetThisPage(fileID, page, &pageHandle);
		if (temp != SUCCESS)
			return temp;
		int offset = fileHandle->fileSubHeader->firstRecordOffset;
		int size = fileHandle->fileSubHeader->recordSize;
		int slotNum = fileHandle->fileSubHeader->recordsPerPage;
		for (slot = 0; slot < slotNum; ++slot) {
			if((pageHandle.pFrame->page.pData[slot / 8] & 1 << (slot % 8)) == 0)
				break;
		}
		memcpy(pageHandle.pFrame->page.pData + offset + size * slot, pData, size);
		pageHandle.pFrame->page.pData[slot / 8] |= (1 << (slot % 8));

		if (isFull(slot + 1, slotNum, pData, &pageHandle)) {//添加后该页满
			fileHandle->pBitmap[page / 8] |= (1 << (page % 8));
		}

		MarkDirty(&pageHandle);
		UnpinPage(&pageHandle);
		rid->bValid = true;
		rid->pageNum = page;
		rid->slotNum = slot;
	}
	
	fileHandle->fileSubHeader->nRecords++;
	fileHandle->pHdrFrame->bDirty = true;
	return SUCCESS;
	
}

bool isFull(int slot,int slotNum,char *pData,PF_PageHandle *pageHandle) {
	for (int i = slot; i < slotNum; ++i) {
		if ((pageHandle->pFrame->page.pData[i / 8] & 1 << (i % 8)) == 0)//后续有空
			return false;
	}
	return true;
}



RC DeleteRec(RM_FileHandle *fileHandle, const RID *rid)
{
	return FAIL;
}

/*
更新指定文件中的记录，rec指向的记录结构中的rid字段为要更新的记录的标识符，pData字段指向新的记录内容。
*/
RC UpdateRec(RM_FileHandle *fileHandle, const RM_Record *rec)
{
	return FAIL;
}

RC RM_CreateFile(char *fileName, int recordSize)
{

	int fileID;
	int sizeRM = sizeof(RM_FileSubHeader);

	RC temp = CreateFile(fileName);
	if (temp != SUCCESS)
		return temp;

	temp = OpenFile(fileName, &fileID);
	if (temp != SUCCESS)
		return temp;

	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	temp = AllocatePage(fileID, pageHandle);
	if (temp != SUCCESS)
		return temp;

	char* bitMap = pageHandle->pFrame->page.pData + sizeRM;
	bitMap[0] = 0x3;//0 1页为满

	RM_FileSubHeader* fileSubHeader = (RM_FileSubHeader*)pageHandle->pFrame->page.pData;
	fileSubHeader->nRecords = 0;
	fileSubHeader->recordSize = recordSize;
	fileSubHeader->recordsPerPage = PF_PAGE_SIZE / (recordSize + 1.0 / 8);//每个页面可以装载的记录数量
	fileSubHeader->firstRecordOffset = ceil((double)fileSubHeader->recordsPerPage / 8.0);//每页第一个记录在数据区中的开始位置

	temp = CloseFile(fileID);
	if (temp != SUCCESS)
		return temp;

	temp = MarkDirty(pageHandle);
	if (temp != SUCCESS)
		return temp;
	free(pageHandle);
	return SUCCESS;
}


/*
创建一个名为fileName的记录文件，该文件中每条记录的大小为recordSize。
*/
RC RM_OpenFile(char *fileName, RM_FileHandle *fileHandle)
{
	return FAIL;
	int sizeRM = sizeof(RM_FileSubHeader);
	PF_FileHandle* pfHandle = (PF_FileHandle*)malloc(sizeof(PF_FileHandle));
	int fileID;
	RC temp = OpenFile(fileName, &fileID);
	if (temp != SUCCESS) {
		free(pfHandle);
		return temp;
	}
	PF_PageHandle *pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	pageHandle->bOpen = false;
	GetThisPage(fileID, 1, pageHandle);
	if (temp != SUCCESS) {
		free(pfHandle);
		free(pageHandle);
		return temp;
	}

	fileHandle->bOpen = true;
	fileHandle->fileName = fileName;
	fileHandle->fileDesc = fileID;
	fileHandle->pHdrFrame = pageHandle->pFrame;
	fileHandle->pHdrPage = &(pageHandle->pFrame->page);
	fileHandle->pBitmap = pageHandle->pFrame->page.pData + sizeRM;
	fileHandle->fileSubHeader = (RM_FileSubHeader*)pageHandle->pFrame->page.pData;

	free(pfHandle);
	free(pageHandle);
	return SUCCESS;
}

RC RM_CloseFile(RM_FileHandle *fileHandle)
{
	fileHandle->pHdrFrame->pinCount--;
	RC temp = CloseFile(fileHandle->fileDesc);
	if (temp != SUCCESS)
		return temp;
	fileHandle->bOpen = false;
	return SUCCESS;
}