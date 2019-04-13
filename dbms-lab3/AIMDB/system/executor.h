/**
 * @file    executor.h
 * @author  liugang(liugang@ict.ac.cn)
 * @version 0.1
 *
 * @section DESCRIPTION
 *  
 * definition of executor
 *
 */

#ifndef _EXECUTOR_H
#define _EXECUTOR_H

#include "catalog.h"
#include "mymemory.h"

/** aggrerate method. */
enum AggrerateMethod {
    NONE_AM = 0, /**< none */
    COUNT,       /**< count of rows */
    SUM,         /**< sum of data */
    AVG,         /**< average of data */
    MAX,         /**< maximum of data */
    MIN,         /**< minimum of data */
    MAX_AM
};

/** compare method. */
enum CompareMethod {
    NONE_CM = 0,
    LT,        /**< less than */
    LE,        /**< less than or equal to */
    EQ,        /**< equal to */
    NE,        /**< not equal than */
    GT,        /**< greater than */
    GE,        /**< greater than or equal to */
    LINK,      /**< join */
    MAX_CM
};

/** definition of request column. */
struct RequestColumn {
    char name[128];    /**< name of column */
    AggrerateMethod aggrerate_method;  /** aggrerate method, could be NONE_AM  */
};

/** definition of request table. */
struct RequestTable {
    char name[128];    /** name of table */
};

/** definition of compare condition. */
struct Condition {
    RequestColumn column;   /**< which column */
    CompareMethod compare;  /**< which method */
    char value[128];        /**< the value to compare with, if compare==LINK,value is another column's name; else it's the column's value*/
};

/** definition of conditions. */
struct Conditions {
    int condition_num;      /**< number of condition in use */
    Condition condition[4]; /**< support maximum 4 & conditions */
};

/** definition of selectquery.  */
class SelectQuery {
  public:
    int64_t database_id;           /**< database to execute */
    int select_number;             /**< number of column to select */
    RequestColumn select_column[4];/**< columns to select, maximum 4 */
    int from_number;               /**< number of tables to select from */
    RequestTable from_table[4];    /**< tables to select from, maximum 4 */
    Conditions where;              /**< where meets conditions, maximum 4 & conditions */
    int groupby_number;            /**< number of columns to groupby */
    RequestColumn groupby[4];      /**< columns to groupby */
    Conditions having;             /**< groupby conditions */
    int orderby_number;            /**< number of columns to orderby */
    RequestColumn orderby[4];      /**< columns to orderby */
};  // class SelectQuery

/** definition of result table.  */
class ResultTable {
  public:
    int column_number;       /**< columns number that a result row consist of */
    BasicType **column_type; /**< each column data type */
    char *buffer;         /**< pointer of buffer alloced from g_memory */
    int64_t buffer_size;  /**< size of buffer, power of 2 */
    int row_length;       /**< length per result row */
    int row_number;       /**< current usage of rows CURRENT NUMBER OF ROW*/
    int row_capicity;     /**< maximum capicity of rows according to buffer size and length of row  MAXIMUN OF ROW */
    int *offset;
    int offset_size;

    /**
     * init alloc memory and set initial value
     * @col_types array of column type pointers
     * @col_num   number of columns in this ResultTable
     * @param  capicity buffer_size, power of 2
     * @retval >0  success
     * @retval <=0  failure
     */
    int init(BasicType *col_types[],int col_num,int64_t capicity = 1024);
    /**
     * calculate the char pointer of data spcified by row and column id
     * you should set up column_type,then call init function
     * @param row    row id in result table
     * @param column column id in result table
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    char* getRC(int row, int column);
    /**
     * write data to position row,column
     * @param row    row id in result table
     * @param column column id in result table
     * @data data pointer of a column
     * @retval !=NULL pointer of a column
     * @retval ==NULL error
     */
    int writeRC(int row, int column, void *data);
    /**
     * print result table, split by '\t', output a line per row 
     * @retval the number of rows printed
     */
    int print(void);
    /**
     * write to file with FILE *fp
     */
    int dump(FILE *fp);
    /**
     * free memory of this result table to g_memory
     */
    int shut(void);
};  // class ResultTable



/** definition of Operator.  */
class Operator {
    protected:
        ResultTable result;         /**< each operator got its own ResultTable(Buffer) except Scan Operator. */
    public:
        RowTable *output_table;           /**< it tells pattern and column imformation of output table,but not store records   */
        int64_t Ope_id = 0;         /**< for DEBUG use. */
        /**
         * construction of Operator.
         */
        Operator(void) {}
        /**
         *  operator init
         *  @retval true  success 
         *  @retval false failure 
         */
        virtual bool    init    () = 0; 
        /**
         * get next record and put it in resulttable 
         * @param result buffer to store the record 
         * @retval true  for success 
         * @retval false for failure 
         */
        virtual bool    getNext (ResultTable *result) = 0;
        /**
         * where this operator is end
         * @retval true  run end
         * @retval false not end
         */
        virtual bool    isEnd   () = 0;
        /**
         * close the operator and release memory
         * @retval true  for success
         * @retval false for failure 
         */
        virtual bool    close   () = 0;
};
/** definition of class executor.  */
class Executor {
  private:
    SelectQuery *current_query = NULL;     /**< selectquery to iterately execute.     */
    Operator    *top_op = NULL;            /**< Top operator of the operator tree.    */
    BasicType   **result_type;             /**< Type of every column in result table. */
    int64_t     filter_tid[4];             /**< Table id of filter conditions.        */
    Condition   *join_cond[4];             /**< Conditions for join.                  */
    int64_t     count;                     /**< count records for debug uses          */
  public:
    /**
     * exec function.
     * @param  query to execute, if NULL, execute query at last time 
     * @result result table generated by an execution, store result in pattern defined by the result table
     * @retval >0  number of result rows stored in result
     * @retval <=0 no more result
     */
    virtual int exec(SelectQuery *query, ResultTable *result);
    /**
     * close function.
     * @param None
     * @retval ==0 succeed to close
     * @retval !=0 fail to close
     */
    virtual int close(); 
    /**
     * get table id of each operator.
     * @param query what is query.
     * @param i conditions number
     * @param tid data structure address of store table id
     * @param signal control signal
     */
    virtual void* find(SelectQuery *query,int i,int64_t *tid,int signal);
};//class Executor



/** definition of Scan operator. */
class Scan : public Operator {
    private:
        int64_t scan_cnt;       /**< number of times the scan operation is performed. */
        RowTable* scan_table;   /**< Scanned table.*/
	public:
        /**
         * Constructor of Scan operator. 
         * @param table_name the rowtable scanned. 
         */
        Scan(char *table_name);
        /**
         * Initialize data structures and allocate resources.
         * @retval false failure 
         * @retval true  success 
         */
        bool	init	();
        /**
         * Get the next record of scan operator.
         * @param result the buffer to cache record of scan operator, only one row of data is returned per call. 
         * @retval false failure 
         * @retval true  success 
         */
        bool    getNext (ResultTable *result);
        /**
         * Determine when the scan operator end.
         * @retval false not end
         * @retval true  end
         */
        bool    isEnd   ();
        /**
         * Release resources occupied by scan operator.
         * @retval false failure 
         * @retval true  success 
         */
        bool    close   ();
};

/** definition of filter operaotr*/
class Filter : public Operator {
    private:
        Operator *prior_op;         /**< operator of prior operation.   */
        RowTable *filter_table;     /**< filtered table                 */
        Condition filter_cond;      /**< filter condition               */
        BasicType **col_type;       /**< filter table column type       */
    public:
        /**
         * Constructor of filter Operator
         * @param op prior op that current op calls functions to obtain records.
         * @param cond filter condition.
         */
        Filter(Operator *op, Condition *cond);
        /**
         * Initialize data structures and allocate resources.
         * @retval false failure 
         * @retval true  success 
         */
        bool    init    (); 
        /**
         * Get the next record of filter operator.
         * @param result the buffer to cache record of filter operator, only one row of data is returned per call. 
         * @retval false failure 
         * @retval true  success 
         */
        bool    getNext (ResultTable *result);
        /**
         * Determine when the filter operator end.
         * @retval false not end
         * @retval true  end
         */
        bool    isEnd   ();
        /**
         * Release resources occupied by filter operator.
         * @retval false failure 
         * @retval true  success 
         */
        bool    close   ();
}; 

/** definition of Join. */
class Join : public Operator {
    private:
        Operator *op[2] = {NULL, NULL};    /**< prior operator from tow join table. */
        int op_num = 0;                    /**< number of join table                */
        int cond_num = 0;                  /**< number of join conditions           */ 
        int col_num[2] = {0, 0};           /**< the column numbers of two table    */
        int64_t col_rank[2] = {-1,-1};     /**< which colnum of two table used by join operator*/  
        ResultTable temp_table[200000];    /**< tables for temporarily storing data*/
        int table_num = 0;                 /**< number of temporary tables*/
        int current_table;                 /**< which is temporary tables joining */ 
        BasicType * value_type;            /**< result type of result              */
        BasicType** col_type[2];           /**< the column types of two table*/
        RowTable *join_table[4];           /**< tables joined */ 
        int64_t restab_colnum = 0;         /**< column number of output table*/ 

        

    public:
        /**
         * Constructor of join operator. 
         * @param op_num number of tables involved in join operator.
         * @param op prior op that current op calls functions to obtain records.
         * @param cond_num condition number of join operator. 
         * @param cond condtions of join operator
         */
        Join(int op_num, Operator **op, int cond_num, Condition *cond);
        /**
         * Initialize data structures and allocate resources.
         * @retval false failure 
         * @retval true  success 
         */
        bool    init    ();
        /**
         * Get the next record of join operator.
         * @param result the buffer to cache record of join operator, only one row of data is returned per call. 
         * @retval false failure 
         * @retval true  success 
         */
        bool    getNext (ResultTable *result);
        /**
         * Determine when the join operator end.
         * @retval false not end
         * @retval true  end
         */
        bool    isEnd   ();
        /**
         * Release resources occupied by join operator.
         * @retval false failure 
         * @retval true  success 
         */
        bool    close   (); 
};

/** definition of project   */
class Project : public Operator {
    private:
        Operator *prior_op;         /**< prior operators                      */
        RowTable *project_table;    /**< projected table                      */
        BasicType **col_type;       /**< table column type                    */
        int64_t col_num;            /**< number of columns being projected.   */
        int64_t col_rank[4];        /**< rank sets of columns projected       */
    public:
        /**
         * Constructor of project operator 
         * @param op prior op that current op calls functions to obtain records.
         * @param col_tot total columns projected 
         * @param cols_name columns name of projected column 
        */
        Project(Operator *op, int64_t col_num, RequestColumn *col_name);
        /** 
         * Initialize data structures and allocate resources.
         * @retval false failure 
         * @retval true  success 
         */
        bool    init    ();
        /**
         * Get the next record of project operator.
         * @param result the buffer to cache record of project operator, only one row of data is returned per call. 
         * @retval false failure 
         * @retval true  success 
         */
        bool    getNext (ResultTable *result);
        /**
         * Determine when the project operator end.
         * @retval false not end
         * @retval true  end
         */
        bool    isEnd   (); 
        /**
         * Release resources occupied by project operator.
         * @retval false failure 
         * @retval true  success 
         */
        bool    close   ();
};

/** definiton of OrderBy   */
class OrderBy: public Operator{
        private:
                Operator *prior_op;         /**< prior operators                      */
                int64_t index = 0;          /**<the number that has ordered           */
                int64_t record_size=0;      /**<size of each records                  */
                int64_t orderbynum;         /**<the conditions need to be ordered     */
                RowTable *orderby_table[4]; /**<the table needed to be ordered        */
                RequestColumn *cols_name;   /**<the RPattern of the input table       */
                ResultTable temp_table;     /** to store temp result table            */
        public:
            /**
            * construction of OrderBy
            * @param op prior operators 
            * @param Orderbynum number of condtions in order 
            * @param cols_name names of columns 
            */
            OrderBy(Operator * op, int64_t orderbynum, RequestColumn *cols_name);
            /**
            * quick_sort
            * @param buffer temp buffer to store the data 
            * @param left left side of the order list 
            * @param right right side of the order list
            */
            void quick_sort(char *buffer, int64_t left, int64_t right);
             /**
            * compare tow buffers 
            * @param l first buffer
            * @param r second buffer 
            */
            int compare(void *l, void *r);
            /**
            *initial of the project
            */
            bool init();
            /**
            * get next record of operator 
            * @param result buffer to store result 
            * @retval false for failure 
            * @retval true  for success 
            */
            bool getNext(ResultTable *result);
            /**
            *end or not
            */
            bool isEnd();
            /**
            *close and free
            */
            bool close();

};


/** definition of GroupBy */
class GroupBy : public Operator {
    private:
        Operator *prior_op;                 /**< prior operator                         */
        int64_t row_size;                   /**<  row size                              */        
        int aggre_number = 0;               /**< number of aggrerated columns           */          
        int non_aggre_num = 0;              /**< number of no aggrerated columns        */ 
        BasicType *aggre_type[4];           /**< type of aggrerated columns             */     
        BasicType *non_aggre_type[4];       /**< type of no aggrerated columns          */ 
        size_t aggre_off[4];                /**< offset of aggrerated columns           */           
        size_t non_aggre_off[4];            /**< offset of no aggrerated columns        */ 
        int aggrerate_index = 0;            /**< index of aggrerated column             */       
        int non_aggre_index = 0;            /**< index of no aggrerated column          */
        AggrerateMethod aggre_method[4];    /**< aggrerate methods                      */       
        ResultTable buffer_1;               /**< buffer1 to cache result                */ 
        ResultTable buffer_2;               /**< buffer2 to cache result                */ 
        int col_num = 0;                    /**< column number                          */ 
        int index = 0;                      /**< current index                          */
        RowTable *group_table;              /**< table groupby                          */
        BasicType **col_type;               /**< output table column types              */
        
        /**
         * do aggregate
         * @param agg_method aggregaion method
         * @param prob_result the result of hashtable::probe
         * @param agg_i the index of aggregation
         */
        bool aggre(AggrerateMethod agg_method, uint64_t prob_result, int agg_i);
        /**
         * get hash number 
         * @param key according to value to get hash number. 
         * @param type the type of column which value located.
         */
        uint32_t gethashnum(char *key, BasicType * type);
    public:
        /**
         * Constructor of GroupBy
         * @param op prior op that current op calls functions to obtain records.
         * @param groupby_num number of condtions to groupby 
         * @param req_col names of columns 
         */
        GroupBy(Operator *op, int group_number, RequestColumn req_col[4]);
        /** 
         * Initialize and allocate.
         */
        bool    init();
        /**
         * Get the next record of operator.
         * @param result_buf is the buffer to record project operator 
         */
        bool getNext(ResultTable *result_buf);
        /**
         *end or not
         */
        bool    isEnd() ;
        /**
         * close and free
         */
        bool    close();
};


#endif
