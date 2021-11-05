from functions import *

if __name__ == '__main__':

    thread_customer=ThreadWithReturn(target=Load, args=('data/customer.tbl', [int, str, str, int, str, float, str, str],))
    thread_customer.start()
    thread_lineitem=ThreadWithReturn(target=Load, args=('data/lineitem.tbl', [int]*5 + [float]*3 + [str]*8,))
    thread_lineitem.start()
    thread_nation=ThreadWithReturn(target=Load, args=('data/nation.tbl', [int, str, int, str],))
    thread_nation.start()
    thread_orders=ThreadWithReturn(target=Load, args=('data/orders.tbl', [int, int, str, float] + [str]*3 + [int, str],))
    thread_orders.start()
    thread_part=ThreadWithReturn(target=Load, args=('data/part.tbl', [int] + [str]*4 + [int, str, float, str],))
    thread_part.start()
    thread_partsupp=ThreadWithReturn(target=Load, args=('data/partsupp.tbl', [int]*3 + [float, str],))
    thread_partsupp.start()
    thread_region=ThreadWithReturn(target=Load, args=('data/region.tbl', [int, str, str],))
    thread_region.start()
    thread_supplier=ThreadWithReturn(target=Load, args=('data/supplier.tbl', [int, str, str, int, str, float, str],))
    thread_supplier.start()
    REGION = thread_region.join()
    PARTSUPP = thread_partsupp.join()
    PART = thread_part.join()
    ORDERS = thread_orders.join()
    NATION = thread_nation.join()
    LINEITEM = thread_lineitem.join()
    CUSTOMER = thread_customer.join()
    SUPPLIER = thread_supplier.join()

    print("START")

    # HASH JOIN

    start = time.time()
    JOINED = hash_join_thread(PART, 'P_PARTKEY', CUSTOMER, 'C_CUSTKEY', nb_thread=4)
    end = time.time()
    print('HASH JOIN THREADED :', format(end - start, '.4f'))

    start = time.time()
    JOINED = hash_join(CUSTOMER, 'C_CUSTKEY', PART, 'P_PARTKEY')
    end = time.time()
    print('HASH JOIN :', format(end - start, '.4f'))

    write(JOINED, 'JOINED.csv')

    # PROJECTION

    start = time.time()
    PROJECTION = projection(PART, ['P_PARTKEY', 'P_NAME'])
    end = time.time()
    print('PROJECTION :', format(end - start, '.4f'))

    write(PROJECTION, 'PROJECTION.csv')

    # SELECTIONS

    start = time.time()
    SELECTION = selection(PART, ['P_PARTKEY', 'P_SIZE'], ['<=', '<='], [10, 40])
    end = time.time()
    print('SELECTION :', format(end - start, '.4f'))

    start = time.time()
    SELECTION = selection_thread(PART, ['P_PARTKEY', 'P_SIZE'], ['<=', '<='], [10, 40], nb_thread=4)
    end = time.time()
    print('SELECTION THREADED :', format(end - start, '.4f'))

    write(SELECTION, 'SELECTION.csv')

    start = time.time()
    SELECTION_ATTRIBUTES = selection_attributes(JOINED, ['P_PARTKEY'], ['='], ['C_CUSTKEY'])
    end = time.time()
    print('SELECTION ATTRIBUTES :', format(end - start, '.4f'))

    start = time.time()
    SELECTION_ATTRIBUTES = selection_attributes_thread(JOINED, ['P_PARTKEY'], ['='], ['C_CUSTKEY'], nb_thread=4)
    end = time.time()
    print('SELECTION ATTRIBUTES  THREADED:', format(end - start, '.4f'))

    write(SELECTION_ATTRIBUTES, 'SELECTION_ATTRIBUTES.csv')

    # GROUP-BY

    start = time.time()
    GROUP_BY = group_by(NATION, ['N_REGIONKEY'],[(sum,'N_NATIONKEY')],["N_REGIONKEY"])
    end = time.time()
    print('GROUP BY :', format(end - start, '.4f'))

    start = time.time()
    GROUP_BY = group_by_thread(NATION, ['N_REGIONKEY'], [(sum,'N_NATIONKEY')], ["N_REGIONKEY"], nb_thread=4)
    end = time.time()
    print('GROUP BY THREADED :', format(end - start, '.4f'))

    write(GROUP_BY, 'GROUP_BY.csv')