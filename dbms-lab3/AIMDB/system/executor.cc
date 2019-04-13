/**
 * @file    executor.cc
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */
#include "executor.h"
using namespace std;
int64_t newtable_id = 555;

/** exeutor function */
void* Executor::find(SelectQuery *query,int i,int64_t *tid,int signal){
    int j;
    Column* col= (Column *)g_catalog.getObjByName(query->where.condition[i].column.name);
    int64_t cid = col->getOid();
    for(int j = 0; j < query->from_number; j++){
        Table* table = (Table *)g_catalog.getObjByName(query->from_table[j].name);
        auto columns = table->getColumns();
        for(int k = 0; k < (int)columns.size(); k++){
            if(cid == columns[k]&&signal==0){
                tid[i] = table->getOid();
                break; 
            }else if(cid == columns[k]&&signal==1){
                tid[i]=j;
                break;
            }
        }
    }    
}

int Executor::exec(SelectQuery *query, ResultTable *result){
    if(query != NULL) {
        int join_count = 0; 
        count = 0;          

        int64_t filter_tid[4] = { -1, -1, -1, -1 }; 
        int64_t having_tid[4] = { -1, -1, -1, -1 }; 
        int64_t joinA_tid[4] = { -1, -1, -1, -1 };
        int64_t joinB_tid[4] = { -1, -1, -1, -1 };
        Condition *join_condition[4];                
                
        //filter
        for(int i = 0; i <query->where.condition_num; i++){
            if(query->where.condition[i].compare != LINK){         
                find(query,i,filter_tid,0);
            }
        }

        //join 
        for(int i = 0; i < query->where.condition_num; i++){
            if(query->where.condition[i].compare == LINK){
                join_condition[join_count] = &query->where.condition[i];
                find(query,join_count,joinB_tid,1);
                find(query,join_count,joinA_tid,1);
                join_count++;
            }
        }
		
        //having
        for (int i = 0; i < query->having.condition_num; i++) {            
            find(query,i,having_tid,0);
        }

        // build operator tree
        Operator *op[4];
        for(int i = 0; i < query->from_number; i++){
             RowTable *row_table = (RowTable *)g_catalog.getObjByName(query->from_table[i].name);
            op[i] = new Scan(query->from_table[i].name);
            int64_t row_tid = row_table->getOid();
            for(int j = 0; j < 4; j++){
                if(filter_tid[j] == row_tid) {
                    op[i] = new Filter(op[i], &query->where.condition[j]); 
                }
                if(having_tid[j] == row_tid) {
                    op[i] = new Filter(op[i], &query->having.condition[j]); 
                }
            }
        }
		Operator *op_l;
		if(join_count>1) {
			Operator ***hash_op = new Operator **[4];
			for (int i = 0; i < join_count; i++) {
				hash_op[i] = new Operator*[2];
				hash_op[i][0] = op[joinA_tid[i]];
				hash_op[i][1] = op[joinB_tid[i]];
				op_l = new Join(2, hash_op[i], 1, join_condition[i]);
            
				op[joinA_tid[i]] = op_l;
				op[joinB_tid[i]] = op_l;
			}
		}else if (join_count == 1) {
			op_l = new Join(2, op, 1, join_condition[0]);
		}else{
			op_l = op[0];
		}

		if(query->select_number){
			op_l = new Project(op_l, query->select_number, query->select_column);
		}
		if(query->groupby_number){
			op_l = new GroupBy(op_l, query->select_number, query->select_column);
		}
		if(query->orderby_number){
			op_l = new OrderBy(op_l, query->orderby_number, query->orderby);
		}
		//init operator
		top_op = op_l;
		top_op->init();
    }
    // build result table
    int col_num = (int) top_op->output_table->getColumns().size();
    auto row_pattern =top_op->output_table->getRPattern();
    result_type = new BasicType *[col_num];
    for(int i=0; i < col_num; i++)
        result_type[i] = row_pattern.getColumnType(i);

    result->init(result_type, col_num, 2048); 
    ResultTable final_temp_result;
    result->row_number = 0; 
    final_temp_result.init(result_type, col_num , 2048); 

    //write result table 
    while(result->row_number < 1024/result->row_length && top_op->getNext(&final_temp_result)){  
        for(int j = 0; j < col_num; j++){
            char* buf = final_temp_result.getRC(0, j);
            result->writeRC(result->row_number, j, buf);
        }
        result->row_number ++;
    }
    count += result->row_number;
    
    //end 
    if(result->row_number == 0) {
        top_op->close();
        return 0;
    }
    return 1;
}

int Executor::close()
{
    delete top_op;
    delete [] result_type;
	return 1;
}


// note: you should guarantee that col_types is useable as long as this ResultTable in use, maybe you can new from operate memory, the best method is to use g_memory.
int ResultTable::init(BasicType *col_types[], int col_num, int64_t capicity) {
    column_type = col_types;
    column_number = col_num;
    row_length = 0;
    buffer_size = g_memory.alloc (buffer, capicity);
    if(buffer_size != capicity) {
        printf ("[ResultTable][ERROR][init]: buffer allocate error!\n");
        return -1;
    }
    int allocate_size = 1;
    int require_size = sizeof(int)*column_number; 
    while (allocate_size < require_size)
        allocate_size = allocate_size << 1;
    if(allocate_size < 8)
    allocate_size *=2;
    char *p = NULL;
    offset_size = g_memory.alloc(p, allocate_size);
    if (offset_size != allocate_size) {
        printf ("[ResultTable][ERROR][init]: offset allocate error!\n");
        return -2;
    }
    offset = (int*) p;
    for(int ii = 0;ii < column_number;ii ++) {
        offset[ii] = row_length;
        row_length += column_type[ii]->getTypeSize(); 
    }
    row_capicity = (int)(capicity / row_length);
    row_number   = 0;
    return 0;
}

/** print */
int ResultTable::print (void) {
    int row = 0;
    int ii = 0;
    char buffer[1024];
    char *p = NULL; 
    while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            printf("%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        printf("%s\n", buffer);
        row ++; ii=0;
    }
    return row;
}

/** dump result to fp*/
int ResultTable::dump(FILE *fp) {
    int row = 0;
    int ii = 0;
    char *buffer;
    g_memory.alloc(buffer, 128);
    char *p = NULL; 
   while(row < row_number) {
        for( ; ii < column_number-1; ii++) {
            p = getRC(row, ii);
            column_type[ii]->formatTxt(buffer, p);
            fprintf(fp,"%s\t", buffer);
        }
        p = getRC(row, ii);
        column_type[ii]->formatTxt(buffer, p);
        fprintf(fp,"%s\n", buffer);
        row ++; ii=0;
    }
    g_memory.free(buffer,128);
    return row;
}

// this include checks, may decrease its speed
char* ResultTable::getRC(int row, int column) {
    return buffer+ row*row_length+ offset[column];
}

/** write rc with data*/
int ResultTable::writeRC(int row, int column, void *data) {
    char *p = getRC (row,column);
    if (p==NULL) return 0;
    return column_type[column]->copy(p,data);
}

/** shut memory */
int ResultTable::shut (void) {
    // free memory
    g_memory.free (buffer, buffer_size);
    g_memory.free ((char*)offset, offset_size);
    return 0;
}

//---operators implementation---

//-  -------------------------Scan-----------------------------------------
Scan::Scan(char*tablename) {
    this->scan_table = (RowTable *)g_catalog.getObjByName(tablename);;
    this->output_table = scan_table;
}

bool Scan::init(void) {
    this->scan_cnt = 0;
    return true;
}

bool Scan::getNext(ResultTable *result) {
    char* buffer ;
    g_memory.alloc(buffer, 128);
    for (int i = 0; i < this->scan_table->getColumns().size(); i++) {
        this->scan_table->selectCol(scan_cnt, i, buffer);
        result->writeRC(0, i, buffer);
    }
    this->scan_cnt++;
    g_memory.free(buffer, 128);
    return true;
}

bool Scan::isEnd(void) {
    if(this->scan_cnt == this->scan_table->getRecordNum()) return true;
    else return false;
}

bool Scan::close() {
    this->scan_cnt = 0;
    return true; 
}


//-----------------------------------Filter------------------------------------------------
Filter::Filter(Operator *op, Condition *cond) {
    this->prior_op = op;
    this->filter_cond = *cond;
    this->filter_table = prior_op->output_table;
    this->output_table = this->filter_table;
}

bool Filter::init() {
    if(prior_op->init()){
        //init result table
        int col_num = this->filter_table->getColumns().size();
        this->col_type = new BasicType *[col_num];
        for(int i=0; i < col_num; i++)
            this->col_type[i] = this->filter_table->getRPattern().getColumnType(i);
        if(!this->result.init(this->col_type, col_num)) {
            return true;
        }
        else {return false;}
    }
    else {return false;}

}

bool Filter::getNext(ResultTable *result) {
    //get the value to compare with
    Column *col = (Column*)g_catalog.getObjByName(this->filter_cond.column.name);
    int64_t col_rank = this->filter_table->getColumnRank(col->getOid());
    BasicType *col_type = this->filter_table->getRPattern().getColumnType(col_rank);
    //change value 
    char *const_value;
    g_memory.alloc(const_value, 128);
    col_type->formatBin(const_value, this->filter_cond.value); 

    bool flag = false;
    char *buffer1;
    char *buffer2;
    g_memory.alloc(buffer1, 128);
    g_memory.alloc(buffer2, 128);
    while (false==flag && false==prior_op->isEnd()) {
        if (!prior_op->getNext(&this->result)) return false;

        char*  variable = this->result.getRC(0, col_rank); 
        char*  constant = const_value; 
        col_type->formatTxt(buffer1, variable);
        col_type->formatTxt(buffer2, constant);
        
        switch(this->filter_cond.compare){
            case LT:{
                flag = col_type->cmpLT(variable, constant);
                break;
            }
            case LE:{
                flag = col_type->cmpLE(variable, constant);
                break;
            }
            case EQ:{
                flag = col_type->cmpEQ(variable, constant);
                break;
            }
            case NE:{
                flag = !col_type->cmpEQ(variable, constant);
                break;
            }
            case GT:{
                flag = col_type->cmpGT(variable, constant);
                break;
            }
            case GE:{
                flag = col_type->cmpGE(variable, constant);
                break;
            }
            default:{
                flag = false;
                break;
            }
        }
        
    }
    //If condition meets, copy result from prior_op.result;
    if (flag) {
        char* record;
        for (int i = 0; i < (int)this->filter_table->getColumns().size(); i++) {
            record = this->result.getRC(0L, i);
            result->writeRC(0, i, record);
        }
    }
        g_memory.free(const_value, 128);
        g_memory.free(buffer1, 128);
        g_memory.free(buffer2, 128);
    return flag;
}

bool Filter::isEnd(){
    return prior_op->isEnd();
}

bool Filter::close(){
    bool ret = prior_op->close();
    delete this->prior_op;
    this->result.shut();
    delete [] this->col_type;
    return ret;
}

//---------------------------------------Project----------------------------------
Project::Project(Operator *op, int64_t col_num, RequestColumn *col_name) {
    this->prior_op = op;
    this->col_num = col_num;
    this->project_table = op->output_table;
    
    
    for (int i = 0; i < col_num; i++) {
        Column *col = (Column *)g_catalog.getObjByName(col_name[i].name);
        this->col_rank[i] = this->project_table->getColumnRank(col->getOid());
    }
    //new a rowtable to tell parent op the pattern and column information
      
    char * newtable_name;
    g_memory.alloc(newtable_name, 128);
    strcpy(newtable_name,to_string(newtable_id).c_str());

    
    RowTable *newtable = new RowTable(newtable_id++, newtable_name);    
    newtable->init();
    
    g_memory.free(newtable_name, 128);

    std::vector < int64_t > &col_id = project_table->getColumns();
    newtable->getRPattern().init(col_num);
    for (int i = 0; i < col_num; i++) {
        newtable->getRPattern().addColumn(this->project_table->getRPattern().getColumnType(this->col_rank[i]));
        newtable->addColumn(col_id[this->col_rank[i]]);
    }
    this->output_table = newtable;
}

bool Project::init(){
    if(prior_op->init()){
        //init result table
        int col_num = this->project_table->getColumns().size();
        this->col_type = new BasicType *[col_num];
        for(int i=0; i < col_num; i++)
            this->col_type[i] = this->project_table->getRPattern().getColumnType(i);
        if(!this->result.init(this->col_type, col_num)) {
            return true;
        }
        else {return false;}
    }
    else {return false;}

}
bool Project::getNext(ResultTable *result) {
    char* record ;
    if (!this->prior_op->getNext(&this->result)){
        return false;
    }
    for (int i = 0; i < this->col_num; i++) { 
        record = this->result.getRC(0L, col_rank[i]);
        result->writeRC(0, i, record);
    }
    return true;
}
bool Project::isEnd(){
    return prior_op->isEnd();
}
bool Project::close(){
    bool ret = prior_op->close();
    delete this->prior_op;
    this->result.shut();
    delete [] this->col_type;
    return ret;
}


//------------------------------Join------------------------------

Join::Join(int op_num, Operator **op, int cond_num, Condition *cond) {
    this->op_num = op_num;
    if (op[1]->output_table->getRecordNum()>op[0]->output_table->getRecordNum()) {
        Operator* op_switch = op[1];
        op[1] = op[0];
        op[0] = op_switch;
    }
    for (int i=0; i < op_num; i++) {
        this->op[i] = op[i];
        join_table[i] = op[i]->output_table;
        col_num[i] = (int)join_table[i]->getColumns().size();
        restab_colnum += col_num[i];
    }
    char * col[2];
    int64_t col_id[2];
    col[0] = cond->column.name;
    col_id[0] = g_catalog.getObjByName(col[0])->getOid();

    col[1] = cond->value;
    col_id[1] = g_catalog.getObjByName(col[1])->getOid();
    
    //make sure colA belongs to table_0
    bool flag = true;
    auto & cols_id = join_table[0]->getColumns();
    for (int i = 0; i < col_num[0]; i++) 
        if (cols_id[i]==col_id[0]) {
            flag = false;
            break;
        }
    if (flag) {
        int64_t col_switch_tmp = col_id[0];
        col_id[0] = col_id[1];
        col_id[1] = col_switch_tmp;
    }
    col_rank[0] = join_table[0]->getColumnRank(col_id[0]);
    col_rank[1] = join_table[1]->getColumnRank(col_id[1]);
    //new a rowtable to tell parent op the pattern and column information
      
    char * newtable_name;
    g_memory.alloc(newtable_name, 128);
    strcpy(newtable_name,to_string(newtable_id).c_str());

    
    RowTable *newtable = new RowTable(newtable_id++, newtable_name);    
    newtable->init();
    g_memory.free(newtable_name, 128);
    this->output_table = newtable;

    RPattern *newRPattern = &output_table->getRPattern();
    RPattern *oldRPattern = NULL;
    
    newRPattern->init(restab_colnum);
    
    col_type[0] = new BasicType *[col_num[0]];
    BasicType *t;  
    int cnt = 0;
    for (int i = 0; i < op_num; i++) {
        oldRPattern = &join_table[i]->getRPattern();
        auto &cols_id = join_table[i]->getColumns();
        for (int j = 0; j < col_num[i]; j++) {
            if(cnt<col_num[0])  
                col_type[0][cnt++] = oldRPattern->getColumnType(j);
            t = oldRPattern->getColumnType(join_table[i]->getColumnRank(cols_id[j]));
            newRPattern->addColumn(t);
            output_table->addColumn(cols_id[j]);
        }
    }
    this->result.init(col_type[0], col_num[0]);
}

bool Join::init(void) {
    for(int i = 0; i <op_num; i++)
        op[i]->init();
    col_type[1] = new BasicType * [col_num[1]];
    for (int i=0; i < col_num[1]; i++)
        col_type[1][i] = join_table[1]->getRPattern().getColumnType(i);
    

    while (!op[1]->isEnd()) {
        temp_table[table_num].init(col_type[1], col_num[1]);
        op[1]->getNext(&temp_table[table_num]);
        table_num++ ;
    }
    this->value_type = join_table[0]->getRPattern().getColumnType(col_rank[0]);
    this->current_table =0;
    return true;
}

bool Join::getNext(ResultTable *result) {
    bool flag = false;
    char* cmpSrcA_ptr;
    char* cmpSrcB_ptr;
    char *buffer1;
    char *buffer2;
    g_memory.alloc(buffer1,128);
    g_memory.alloc(buffer2,128);
    //ResultTable *tA;
    ResultTable *tB;
    if(table_num>=150000) return false;
    while (!flag) {
        if(op[0]->isEnd()) break;
        if (current_table==0){
            op[0]->getNext(&this->result);
        }
        cmpSrcA_ptr = this->result.getRC(0, col_rank[0]);
        this->value_type->formatTxt(buffer1,cmpSrcA_ptr);
        while(current_table<table_num){
            cmpSrcB_ptr = temp_table[current_table].getRC(0, col_rank[1]);
            this->value_type->formatTxt(buffer2,cmpSrcB_ptr);
            if(this->value_type->cmpEQ(cmpSrcA_ptr, cmpSrcB_ptr)){
                tB = &temp_table[current_table];
                flag =true;
                current_table++;
                break;
            }
            current_table++;
        }
        if(current_table==table_num){
            current_table =0;
        }
        

        if (flag) break;
    }
    if (flag) {
        char* buffer;
        for (int j = 0; j < col_num[0]; j++) {
            buffer = this->result.getRC(0L, j);
            if (!result->writeRC(0, j, buffer))
            {
                return false;
            } 
        }
        for (int j = 0; j < col_num[1]; j++) {
            buffer = tB->getRC(0L, j);
            if (!result->writeRC(0, col_num[0] + j, buffer))
            {
                return false;
            } 
        }
    }
    g_memory.free(buffer1,128);
    g_memory.free(buffer2,128);
    return flag;
}

bool Join::close(void) {
    delete []col_type[0];
    delete []col_type[1];
    result.shut();
    for (int i = 0; i < table_num; i++) {
        temp_table[i].shut();
    }
    for (int i = 0; i < op_num; i++) 
        if (!op[i]->close()) return false;
    for (int i = 0; i < op_num; i++) 
        delete op[i];
    return false;
}

bool Join::isEnd(void)  {
    return op[0]->isEnd();
}


//----------------------------------OrderBy----------------------------------------

OrderBy::OrderBy(Operator *op, int64_t orderbynum, RequestColumn *cols_name){
   
    this->prior_op=op;
    this->orderby_table[0]=op->output_table;
    this->output_table=this->orderby_table[0];
    this->cols_name=cols_name;
    this->orderbynum=orderbynum;
   
}

bool OrderBy::init(){
    int i,j=0;
    int64_t col_number=this->orderby_table[0]->getColumns().size();
    char *buffer;
    if(prior_op->init()){ 
        
       
        BasicType **in_col_type=new BasicType *[col_number];
        for(i=0;i<col_number;i++){
            in_col_type[i]=this->orderby_table[0]->getRPattern().getColumnType(i);
        }
        this->temp_table.init(in_col_type, col_number, 1024*32);
        this->result.init(in_col_type,col_number);
        while(!prior_op->isEnd() && prior_op->getNext(&this->result)){
            for(i=0;i<col_number;i++){
             buffer=result.getRC(0,i);
             temp_table.writeRC(j,i,buffer);
            }
            j++;
        }
        this->record_size=j;
        quick_sort(temp_table.buffer,0,record_size-1);
        this->index=0;
        free(in_col_type);
        return true;
    }else
        return false;
}

bool OrderBy::getNext(ResultTable *result){
    int i;
    char *buffer;
    int col_number=this->orderby_table[0]->getColumns().size();
    if(this->isEnd())return false;
    for(i=0;i<col_number;i++){
        buffer=this->temp_table.getRC(this->index,i);
        if(!result->writeRC(0,i,buffer))
            return false;
    }
    this->index++;
    return true;
}

bool OrderBy::isEnd(){
    if(index==record_size)
        return true;
    else
        return false;
}

bool OrderBy::close(){
    bool ret = prior_op->close();
    delete prior_op;
    temp_table.shut();
    result.shut();
    return ret;
}

void OrderBy::quick_sort(char *buffer,int64_t left,int64_t right){
    if(left<right){
        int64_t i,last=left;
        int64_t res,row_length;
        res=(left+right)/2;
        row_length=this->orderby_table[0]->getRPattern().getRowSize();
        char *tmp=(char *)malloc(row_length);
        memcpy(tmp,buffer+left*row_length,row_length);
        memcpy(buffer+left*row_length,buffer+res*row_length,row_length);
        memcpy(buffer+res*row_length,tmp,row_length);

        for(i=left+1;i<=right;i++)
            if(compare(buffer+left*row_length,buffer+i*row_length)){
                memcpy(tmp,buffer+last*row_length,row_length);
                memcpy(buffer+last*row_length,buffer+i*row_length,row_length);
                memcpy(buffer+i*row_length,tmp,row_length);
                last++;
            }
        memcpy(tmp,buffer+left*row_length,row_length);
        memcpy(buffer+left*row_length,buffer+last*row_length,row_length);
        memcpy(buffer+last*row_length,tmp,row_length);
        quick_sort(buffer,left,last-1);
        quick_sort(buffer,last+1,right);
        free(tmp);
    }
}

int OrderBy::compare(void *l,void *r){
    int i;
    int64_t cmp_col_rank[4];

    RPattern in_RP=this->orderby_table[0]->getRPattern();   

    for(i=0;i<orderbynum;){
        Object *col=g_catalog.getObjByName(cols_name[i].name);//get the type and id
        cmp_col_rank[i]=this->orderby_table[0]->getColumnRank(col->getOid());//get the column
        
        auto p=l+in_RP.getColumnOffset(cmp_col_rank[i]);
        auto q=r+in_RP.getColumnOffset(cmp_col_rank[i]);

        if(in_RP.getColumnType(cmp_col_rank[i])->cmpEQ(p,q))
            i++;
        else if(in_RP.getColumnType(cmp_col_rank[i])->cmpLE(p, q))
            return 0;
        else
            return 1;
    }
    return 1; //l>r
}


//---------------------------------------GroupBy--------------------------------------
GroupBy::GroupBy(Operator *op, int group_number, RequestColumn req_col[4]){
    this->prior_op = op;
    this->group_table = op->output_table;
    this->output_table = this->group_table;  
    for(int i = 0; i < group_number; i++){
        if(req_col[i].aggrerate_method != NONE_AM){
            this->aggre_method[aggrerate_index] = req_col[i].aggrerate_method;
            this->aggre_off[aggrerate_index] = i;
            this->aggre_type[aggrerate_index] = output_table->getRPattern().getColumnType(i);
            this->aggre_number++;
            this->aggrerate_index++;
        }else{
            this->non_aggre_off[non_aggre_index] = i;
            this->non_aggre_type[non_aggre_index] = output_table->getRPattern().getColumnType(i);
            this->non_aggre_num++;
            this->non_aggre_index++;
        }
    }
}

bool GroupBy::init(){
    float avg_result = 0;
    this->row_size = this->group_table->getRPattern().getRowSize();
    int64_t power = 1;
    while (power < row_size){
        power *= 2;
    }
    this->col_num = this->output_table->getColumns().size();
    HashTable *hashtable = new HashTable(1000000, 4, 0); 
    int64_t hash_count = 0;
    char **prob_result = (char**)malloc(4*power);
    char *src;
    int64_t temp_offset = 0;
    int64_t pattern_count[32768];
    int64_t key;
    char temp_buffer[1024];
    if(prior_op->init()){
        int in_colnum = this->group_table->getColumns().size();
        this->col_type = new BasicType *[in_colnum];
        for(int i=0; i < in_colnum; i++){
            this->col_type[i] = this->group_table->getRPattern().getColumnType(i);
        }
        this->buffer_1.init(this->col_type, in_colnum);  
        this->buffer_2.init(this->col_type, in_colnum,524288);  

        for(int i = 0;i < 32768;i++){
            pattern_count[i] = 0;
        }
        while(!prior_op->isEnd()&&prior_op->getNext(&buffer_1)){
            key = 0;
            temp_offset = 0;
            for(int i = 0; i < non_aggre_num; i++){
                src = buffer_1.getRC(0, non_aggre_off[i]);
                buffer_1.column_type[non_aggre_off[i]]->formatTxt(temp_buffer+temp_offset, src);
                temp_offset += non_aggre_type[i]->getTypeSize();
            }
            key = gethashnum(temp_buffer, NULL);   
        
            if(hashtable->probe(key, prob_result, 4) > 0){
                pattern_count[(uint64_t)prob_result[0]]++;
                for(int i = 0; i < aggre_number; i++){
                    aggre(aggre_method[i], (uint64_t)prob_result[0], i);
                }
            }else{
                hashtable->add(key, (char*)hash_count);
                for(int j = 0; j < col_num; j++){
                    src = buffer_1.getRC(0, j);
                    buffer_2.writeRC(hash_count, j, src);             
                }
                buffer_2.row_number++;   
                pattern_count[hash_count]++;
                hash_count++;
            }
        }

        for(int j = 0; j < hash_count; j++){
            for(int m = 0; m < aggre_number; m++){
                auto data_in = buffer_2.getRC(j, aggre_off[m]);
                if(aggre_method[m] == COUNT){
                    aggre_type[m]->copy(data_in, &pattern_count[j]);
                }else if(aggre_method[m] == AVG){
                    avg_result = *(float *)data_in;
                    memcpy(data_in,(void*)&avg_result,4);               
                }           
            }
        }
        free(prob_result);
        return true;
    }else{
        return false;
    }
}

bool GroupBy::getNext(ResultTable *result_buf){
    if(buffer_2.row_number <= this->index){
        return false;
    }
    for(int j = 0; j < col_num; j++){
        if(!result_buf->writeRC(0, j, buffer_2.getRC(this->index, j))){ 
            return false;
        }
    }
    index++;
    return true;
}

bool GroupBy::isEnd(){
    return prior_op->isEnd();
}

bool GroupBy::close() {
    bool ret = prior_op->close();
    delete prior_op;
    buffer_1.shut();
    buffer_2.shut();
    delete [] this->col_type;
    return ret;
}

bool GroupBy::aggre(AggrerateMethod agg_method, uint64_t prob_result, int agg_i){
    auto data_in = buffer_1.getRC(0, aggre_off[agg_i]);
    auto data_out = buffer_2.getRC(prob_result, aggre_off[agg_i]);
    switch(agg_method){
    case SUM:
    case AVG:{
        if(aggre_type[agg_i]->getTypeCode() == INT8_TC){
            int8_t agg_result = *(int8_t *)data_in + *(int8_t *)data_out;
            memcpy(data_out, (void*)&agg_result, 1);
            return true;
        }else if(aggre_type[agg_i]->getTypeCode() == FLOAT32_TC){
            float agg_result = *(float *)data_in + *(float *)data_out;
            memcpy(data_out, (void*)&agg_result, 4);
            return true;
        }else{
            return false;
        }
    }
    case MAX:
        if(aggre_type[agg_i]->cmpLT(data_out, data_in))
            aggre_type[agg_i]->copy(data_out, data_in);
        break;
    case MIN:
        if(aggre_type[agg_i]->cmpLT(data_in, data_out))
            aggre_type[agg_i]->copy(data_out, data_in);
        break;
    default:
        break;
    }
    return true;
}

uint32_t GroupBy::gethashnum(char *key, BasicType * type){
    uint32_t hash_num = 0;
    for (unsigned  i = 0; i < strlen(key); i++)
        hash_num = 31 * hash_num + key[i];
    return hash_num;
}