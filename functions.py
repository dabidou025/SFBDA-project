import pandas as pd
#import pydoop.hdfs as hdfs
import sys
import time
from collections import defaultdict
from threading import Thread
from threading import Lock
from builtins import super    # https://stackoverflow.com/a/30159479

#https://stackoverflow.com/questions/6893968/how-to-get-the-return-value-from-a-thread-in-python
_thread_target_key, _thread_args_key, _thread_kwargs_key = (
    ('_target', '_args', '_kwargs')
    if sys.version_info >= (3, 0) else
    ('_Thread__target', '_Thread__args', '_Thread__kwargs')
)

class ThreadWithReturn(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._return = None
    
    def run(self):
        target = getattr(self, _thread_target_key)
        if not target is None:
            self._return = target(
                *getattr(self, _thread_args_key),
                **getattr(self, _thread_kwargs_key)
            )
    
    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        return self._return

def Load (file_name, cols_types):
    file = open(file_name, 'r')
    cols_names = file.readline().split('|')
    data = []
    for line in file:
        row = line.split('|')
        to_add = []
        for i, col_type in enumerate(cols_types):
            to_add.append(col_type(row[i]))
        data.append(to_add)
    return pd.DataFrame(data=data, columns=cols_names)

def write(table,nom):
    table.to_csv(nom, sep=',', index=False)

def get_row(table, i):
    ret = []
    for col_name in table.columns:
        ret.append(table[col_name][i])
    return ret

def sum (table, col_name):
    ret = 0.0
    table.reset_index(drop=True, inplace=True)
    for i in range(len(table)):
        ret += table[col_name][i]
    return ret

def sum_thread (table, col_name,nb_thread):
    size=table.shape[0]//nb_thread
    list_of_threads = []
    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=sum, args=(table.loc[size*i:,:], col_name,)))
        else:
            list_of_threads.append(ThreadWithReturn(target=sum, args=(table.loc[size*i:size*(i+1)-1,:], col_name,)))
    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        if i==0:
            res=list_of_threads[i].join()
        else:
            res+=list_of_threads[i].join()
    return res

def average (table, col_name):
    return sum(table, col_name) / len(table)

def average_thread (table, col_name,nb_thread):
    return sum_thread(table, col_name,nb_thread) / len(table)

def projection (table, cols_names):
    return table[cols_names]

# Performs a projection without duplicates
def distinct (table, cols_names):
    tmp = projection(table, cols_names)
    added = []; to_add = []
    for i in range(len(table)):
        curr_tuple = get_row(tmp, i)
        if curr_tuple not in added:
            added.append(curr_tuple)
            to_add.append(get_row(table, i))
    return pd.DataFrame(data=to_add, columns=table.columns)

def distinct_thread(table, cols_names, nb_thread):
    data_lock = Lock()
    size=table.shape[0]//nb_thread
    list_of_threads = []
    added = []; to_add = []

    def distinct_single (table,cols_names):
        tmp = projection(table, cols_names)
        for i in tmp.index.array:
            curr_tuple = get_row(tmp, i)
            data_lock.acquire()
            if curr_tuple not in added:
                added.append(curr_tuple)
                to_add.append(get_row(table, i))
            data_lock.release()

    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=distinct_single, args=(table.loc[size*i:,:],cols_names,)))
        else:
            list_of_threads.append(ThreadWithReturn(target=distinct_single, args=(table.loc[size*i:size*(i+1),:],cols_names,)))
    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        list_of_threads[i].join()
    return pd.DataFrame(data=to_add, columns=table.columns)

'''Group by
    Args :
    table -- pandas.DataFrame
    cols_names -- list column names to add to our returning dataframe
    funs -- list of tuples of (function name, column name to apply the function on)
    to_gb -- list of column names to group by
'''
def group_by (table, cols_names, funs, to_gb):
    to_add = []; deja_vu = []
    for i in range(len(table)):
        curr = get_row(table[to_gb], i)
        if curr not in deja_vu:
            tmp = selection(table, to_gb, ["="]*len(to_gb), curr)
            funs_vals = [f(tmp, col_name) for f, col_name in funs]
            to_add.append(get_row(table[cols_names], i) + funs_vals)
            deja_vu.append(curr)
    to_add_cols_names = cols_names + [col_name for f, col_name in funs]
    return pd.DataFrame(data=to_add, columns=to_add_cols_names)

def group_by_thread (table, cols_names, funs, to_gb,nb_thread):
    to_add = []; deja_vu = []
    for i in range(len(table)):
        curr = get_row(table[to_gb], i)
        if curr not in deja_vu:
            tmp = selection_thread(table, to_gb, ["="]*len(to_gb), curr,nb_thread)
            tmp.reset_index(drop=True, inplace=True)
            funs_vals = [f(tmp, col_name) for f, col_name in funs]
            to_add.append(get_row(table[cols_names], i) + funs_vals)
            deja_vu.append(curr)
    to_add_cols_names = cols_names + [col_name for f, col_name in funs]
    return pd.DataFrame(data=to_add, columns=to_add_cols_names)

'''Selection
    Args :
    table -- pandas.DataFrame
    col -- list of column names
    equality -- list of binary operations, one operation is for example ">="
    cond -- list of conditions on columns
    In SQL, it would look like "WHERE col[i] equality[i] cond[i]" for each i
'''
def selection(table,col,equality,cond):
    to_add = []
    for i in table.index.array:
        flag = True
        for j in range(len(col)):
            if equality[j] == "=" and table[col[j]][i]!=cond[j]:
                    flag = False
            elif equality[j] =="<" and table[col[j]][i] >= cond[j]:
                    flag = False
            elif equality[j] =="<=" and table[col[j]][i] > cond[j]:
                    flag = False
            elif equality[j] ==">" and table[col[j]][i] <= cond[j]:
                    flag = False
            elif equality[j] ==">=" and table[col[j]][i] < cond[j]:
                    flag = False
        if flag:
            to_add.append(get_row(table,i))
    return pd.DataFrame(to_add, columns=table.columns)

def selection_thread(table, col_name, equality, cond, nb_thread):
    size=table.shape[0]//nb_thread
    list_of_threads = []
    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=selection, args=(table.loc[size*i:,:], col_name, equality, cond,)))
        else:
            list_of_threads.append(ThreadWithReturn(target=selection, args=(table.loc[size*i:size*(i+1)-1,:], col_name, equality, cond,)))
    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        if i==0:
            res=list_of_threads[i].join()
        else:
            res=pd.concat([res,list_of_threads[i].join()])
    return res

'''Selection by attributes
    Args :
    table -- pandas.DataFrame
    cols1 -- list of column names
    operations -- list of binary operations, one operation is for example ">="
    cols2 -- list of column names
    In SQL, it would look like "WHERE cols1[i] operations[i] cols2[i]" for each i
'''
def selection_attributes(table, cols1, operations, cols2):
    to_add = []
    table.reset_index(inplace=True, drop=True)
    for i in range(len(table)):
        flag = True
        for j in range(len(operations)):
            if operations[j] == "=" and table[cols1[j]][i] != table[cols2[j]][i]:
                    flag = False
            elif operations =="<" and table[cols1[j]][i] >= table[cols2[j]][i]:
                    flag = False
            elif operations[j] =="<=" and table[cols1[j]][i] > table[cols2[j]][i]:
                    flag = False
            elif operations[j] ==">" and table[cols1[j]][i] <= table[cols2[j]][i]:
                    flag = False
            elif operations[j] ==">=" and table[cols1[j]][i] < table[cols2[j]][i]:
                    flag = False
        if flag:
            to_add.append(get_row(table,i))
    return pd.DataFrame(to_add, columns=table.columns)

def selection_attributes_thread(table, cols1, operations, cols2,nb_thread):
    size=table.shape[0]//nb_thread
    to_add = []
    list_of_threads = []

    def selection_attributes_single(table):
        for i in table.index.array:
            flag = True
            for j in range(len(operations)):
                if operations[j] == "=" and table[cols1[j]][i] != table[cols2[j]][i]:
                        flag = False
                elif operations =="<" and table[cols1[j]][i] >= table[cols2[j]][i]:
                        flag = False
                elif operations[j] =="<=" and table[cols1[j]][i] > table[cols2[j]][i]:
                        flag = False
                elif operations[j] ==">" and table[cols1[j]][i] <= table[cols2[j]][i]:
                        flag = False
                elif operations[j] ==">=" and table[cols1[j]][i] < table[cols2[j]][i]:
                        flag = False
            if flag:
                to_add.append(get_row(table,i))

    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=selection_attributes_single, args=(table.loc[size*i:,:],)))
        else:
            list_of_threads.append(ThreadWithReturn(target=selection_attributes_single, args=(table.loc[size*i:size*(i+1)-1,:],)))

    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        list_of_threads[i].join()

    return pd.DataFrame(to_add, columns=table.columns)

def colname_to_index (table, col_name):
    for i, curr_col_name in enumerate(table.columns):
        if curr_col_name == col_name:
            return i
    return -1

'''Hash Join
    Args :
    table1 -- pandas.DataFrame
    index1 -- int of string representing the column to join by in table1
    table2 -- pandas.DataFrame
    index2 -- int of string representing the column to join by in table2
'''
def hash_join (table1, index1, table2, index2, by_col_name=True):
    if by_col_name:
        index1 = colname_to_index(table1, index1)
        index2 = colname_to_index(table2, index2)
    hm = defaultdict(list)
    for i in range(len(table1)):
        row = get_row(table1, i)
        hm[row[index1]].append(row)
    ret = []
    for i in range(len(table2)):
        row2 = get_row(table2, i)
        for row in hm[row2[index2]]:
            ret.append(row + row2)
    return pd.DataFrame(data=ret, columns= list(table1.columns) +
            list(table2.columns))

def hash_join_thread (table1, index1, table2, index2,nb_thread, by_col_name=True):
    size=table1.shape[0]//nb_thread
    list_of_threads = []
    if by_col_name:
        index1 = colname_to_index(table1, index1)
        index2 = colname_to_index(table2, index2)
    hm = defaultdict(list)

    def adding(table):
        for i in table.index.array:
            row = get_row(table, i)
            hm[row[index1]].append(row)

    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=adding, args=(table1.loc[size*i:,:],)))
        else:
            list_of_threads.append(ThreadWithReturn(target=adding, args=(table1.loc[size*i:size*(i+1)-1,:],)))

    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        list_of_threads[i].join()

    ret = []
    list_of_threads.clear()
    def joining(table):
        for i in table.index.array:
            row2 = get_row(table, i)
            for row in hm[row2[index2]]:
                ret.append(row + row2)
    
    for i in range(nb_thread):
        if i==nb_thread-1:
            list_of_threads.append(ThreadWithReturn(target=joining, args=(table2.loc[size*i:,:],)))
        else:
            list_of_threads.append(ThreadWithReturn(target=joining, args=(table2.loc[size*i:size*(i+1)-1,:],)))

    for i in range(nb_thread):
        list_of_threads[i].start()
    for i in range(nb_thread):
        list_of_threads[i].join()

    return pd.DataFrame(data=ret, columns= list(table1.columns) +
            list(table2.columns))
