#include "IX_Manager.h"

RC CreateIndex(const char * fileName, AttrType attrType, int attrLength)
{
	//创建对应的页面文件
	RC tmp;
	if ((tmp = CreateFile(fileName)) != SUCCESS) {
		return tmp;
	}

	//打开创建的文件
	int fileID;
	if ((tmp = OpenFile((char *)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}


	//获取第一页，加入索引控制信息
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = AllocatePage(fileID, pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_FileHeader* fileHeader = (IX_FileHeader*)pageHandle->pFrame->page.pData;
	fileHeader->attrLength = attrLength;
	fileHeader->attrType = attrType;
	fileHeader->keyLength = attrLength + sizeof(RID);
	fileHeader->rootPage = 1;
	fileHeader->first_leaf = 1;
	fileHeader->order = (PF_PAGE_SIZE - sizeof(IX_FileHeader) - sizeof(IX_Node)) / (2 * sizeof(RID) + attrLength);

	IX_Node* IXNode = (IX_Node*)(pageHandle->pFrame->page.pData + sizeof(IX_FileHeader));
	IXNode->is_leaf = 1;
	IXNode->keynum = 0;
	IXNode->parent = 0;
	IXNode->brother = 0;
	IXNode->keys = pageHandle->pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
	IXNode->rids = (RID*)(IXNode->keys + fileHeader->order * (2 * sizeof(RID) + attrLength));

	//修改页面后调用PF中函数进行处理
	if ((tmp = MarkDirty(pageHandle)) != SUCCESS) {
		return tmp;
	}

	if ((tmp = UnpinPage(pageHandle)) != SUCCESS) {
		return tmp;
	}

	free(pageHandle);

	if ((tmp = CloseFile(fileID)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC OpenIndex(const char * fileName, IX_IndexHandle * indexHandle)
{
	//打开文件
	RC tmp;
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//获取索引控制信息
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = GetThisPage(fileID, 0, pageHandle)) != SUCCESS) {
		return tmp;
	}

	indexHandle->bOpen = true;
	indexHandle->fileID = fileID;
	indexHandle->fileHeader = *(IX_FileHeader*)pageHandle->pFrame->page.pData;

	free(pageHandle);

	return RC();
}

RC CloseIndex(IX_IndexHandle * indexHandle)
{

	RC tmp;
	if ((tmp = CloseFile(indexHandle->fileID)) != SUCCESS) {
		return tmp;
	}
	
	free(indexHandle);
	indexHandle = NULL;

	return RC();
}

RC InsertEntry(IX_IndexHandle * indexHandle, void * pData, const RID * rid)
{
	return RC();
}

RC DeleteEntry(IX_IndexHandle * indexHandle, void * pData, const RID * rid)
{
	return RC();
}

RC OpenIndexScan(IX_IndexScan *indexScan,IX_IndexHandle *indexHandle,CompOp compOp,char *value){
	return SUCCESS;
}

RC IX_GetNextEntry(IX_IndexScan *indexScan,RID * rid){
	return SUCCESS;
}

RC CloseIndexScan(IX_IndexScan *indexScan){
		return SUCCESS;
}

RC GetIndexTree(char *fileName, Tree *index){
		return SUCCESS;
}


