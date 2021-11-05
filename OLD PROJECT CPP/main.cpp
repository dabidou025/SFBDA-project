#include "db_loading.hpp"
int main()
{
    
    time_t begin, end; // time_t is a datatype to store time values.
    time(&begin);

     //1 - table part ************************************************************
    vector<string> cols_names = {
        "P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND",
        "P_TYPE", "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT"};
    vector<string> types = {
        "Integer", "55", "25", "10", "25", "Integer", "10", "Float", "23"};
    /*Table part_t(cols_names, types);
    loading("..\\data\\part.tbl", &part_t);

    //2 - table Supplier *******************************************************
    cols_names = {
        "S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"};
    types = {
        "Integer", "25", "40", "Integer", "15", "Float", "101"};
    Table supplier_t(cols_names, types);
    loading("..\\data\\supplier.tbl", &supplier_t);

    //3 - table partsupp******************************************************************
    cols_names = {
        "PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT"};
    types = {
        "Integer", "Integer", "Integer", "Float", "199"};
    Table partsupp_t(cols_names, types);
    loading("..\\data\\partsupp.tbl", &partsupp_t);

    //4 - table customer******************************************************************
    cols_names = {
        "C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY",
        "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"};
    types = {
        "Integer", "25", "40", "Integer", "15", "Float", "10", "117"};
    Table customer_t(cols_names, types);
    loading("..\\data\\customer.tbl", &customer_t); */

/*     //5 - table orders******************************************************************
    cols_names = {
        "O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE",
        "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPPRIORITY", "O_COMMENT"};
    types = {
        "Integer", "Integer", "1", "Float", "10", "15", "15", "Integer", "79"};
    Table orders_t(cols_names, types);
    loading("..\\data\\orders.tbl", &orders_t); */

/*     //6 - table lineitem******************************************************************
    cols_names = {
        "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER",
        "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG",
        "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT",
        "L_SHIPMODE", "L_COMMENT"};
    types = {
        "Integer", "Integer", "Integer", "Integer", "Float", "Float", "Float", "Float",
        "1", "1", "10", "10", "10", "25", "10", "44"};
    Table lineitem_t(cols_names, types);
    loading("..\\data\\lineitem.tbl", &lineitem_t); */

/*     //7 - table Nation *********************************************************
    cols_names = {
        "N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"};
    types = {
        "Integer", "25", "Integer", "152"};
    Table nation_t(cols_names, types);
    -loading("..\\data\\nation.tbl", &nation_t); */

    //8 - table region ***********************************************************
    cols_names = {
        "R_REGIONKEY", "R_NAME", "R_COMMENT"};
    types = {
        "Integer", "25", "152"};
    Table region_t(cols_names, types);
    loading("..\\data\\region.tbl", &region_t);


    time(&end);

    double difference = difftime(end, begin);
    printf("time taken for function() %.2lf seconds.\n", difference);

    cout <<"Sum of R_regionkey in table rejoin"<< sum("R_REGIONKEY", region_t) << endl;
    cout <<"AVG of R_regionkey in table rejoin"<< avg("R_REGIONKEY", region_t) << endl;

    vector<string> quer = {"R_REGIONKEY", "R_NAME"};
    Table result = projection(quer, region_t);
    result.print();

/*     region_t.print();
    cout<<"On applique le distinct"<<endl;
    distinct(region_t);
    cout<<"Apres le distinct"<<endl;
    region_t.print(); */
    return 0;
}