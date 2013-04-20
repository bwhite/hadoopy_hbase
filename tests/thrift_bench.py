#!/usr/bin/env python
# Useful link: https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/thrift/doc-files/Hbase.html
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
import hadoopy_hbase
from hadoopy_hbase import Hbase, ColumnDescriptor, Mutation
import time
import contextlib
import random

TIMER = {}


def get_clear_timer():
    global TIMER
    cur_timer = TIMER
    TIMER = {}
    return cur_timer


@contextlib.contextmanager
def timer(name):
    st = time.time()
    yield
    TIMER[name] = time.time() - st
    print('[%s]: %s' % (name, TIMER[name]))


def random_string(l):
    s = hex(random.getrandbits(8 * l))[2:]
    if s[-1] == 'L':
        s = s[:-1]
    # Pad with zeros
    if len(s) != l * 2:
        s = '0' * (2 * l - len(s)) + s
    return s.decode('hex')

def remove_table(client, table):
    if table in client.getTableNames():
        try:
            client.disableTable(table)
        except:
            pass
        client.deleteTable(table)

def scanner(client, table, column, num_rows, max_rows):
    with timer('scanner:rows%d-%s' % (num_rows, column)):
        sc = client.scannerOpen('benchtable', '', [column] if column else [])
        for x in xrange(max_rows / num_rows):
            out = client.scannerGetList(sc, num_rows)
            if not out:
                break
        client.scannerClose(sc)

def delete_rows(client, table, max_rows):
    with timer('deleteAllRow:Delete'):
        for x in xrange(max_rows):
            client.deleteAllRow(table, str(x))


def create_table(name, cfs):
    import pexpect
    p = pexpect.spawn('hbase shell')
    p.expect('HBase Shell', timeout=10)
    p.expect(' +', timeout=5)
    p.expect('Type "exit<RETURN>" to leave the HBase Shell', timeout=5)
    p.expect('hbase\(.+\):.+', timeout=5)
    cf_out = ["{NAME => '%s', VERSIONS => 1, BLOCKSIZE => %d, BLOCKCACHE => false, IN_MEMORY => false, COMPRESSION => 'NONE', BLOOMFILTER => 'NONE'}" % (cf['NAME'], cf['BLOCKSIZE']) for cf in cfs]
    p.sendline("create '%s', %s" % (name, ', '.join(cf_out)))
    p.expect('hbase\(.+\):.+', timeout=5)
    print p.before
    p.kill(0)


def simple(client, max_rows, cfparams):
    print('\nsimple')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0'], cfparams)

    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    scanner(client, 'benchtable', '', 10, max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def simple_valsize(client, max_rows, cfparams, value_size):
    print('\nsimple')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0'], cfparams)

    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:0', value=random_string(value_size))])

    scanner(client, 'benchtable', '', 10, max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    delete_rows(client, 'benchtable', max_rows)
    remove_table(client, 'benchtable')


def small_large_1cf(client, max_rows, cfparams):
    print('\nsmall_large_1cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0'], cfparams)

    with timer('mutateRow:Create-small'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    for x in ['cf0:small']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-large'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:large', value=random_string(2 ** 20))])

    for x in ['cf0:small', 'cf0:large', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def small_large_2cf(client, max_rows, cfparams):
    print('\nsmall_large_2cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0', 'cf1'], cfparams)
    
    with timer('mutateRow:Create-small'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:small', value=random_string(2 ** 10))])

    for x in ['cf0:small']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-large'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf1:large', value=random_string(2 ** 20))])

    for x in ['cf0:small', 'cf1:large', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def few_many_1cf(client, max_rows, cfparams):
    print('\nfew_many_1cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0'], cfparams)
    with timer('mutateRow:Create-few'):
        for x in xrange(max_rows):
            if x % 100 == 0:
                client.mutateRow('benchtable', str(x), [Mutation(column='cf0:few', value=random_string(2 ** 10))])

    for x in ['cf0:few']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-many'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:many', value=random_string(2 ** 10))])

    for x in ['cf0:few', 'cf0:many', '']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def few_many_2cf(client, max_rows, cfparams):
    print('\nfew_many_2cf')
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0', 'cf1'], cfparams)
    
    with timer('mutateRow:Create-few'):
        for x in xrange(max_rows):
            if x % 100 == 0:
                client.mutateRow('benchtable', str(x), [Mutation(column='cf0:few', value=random_string(2 ** 10))])

    for x in ['cf0:few']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)
    
    with timer('mutateRow:Create-many'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf1:many', value=random_string(2 ** 10))])

    for x in ['cf0:few', 'cf1:many', '']:
        scanner(client, 'benchtable', x, 100, max_rows)
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def manycols_1cf(client, max_rows, cfparams):
    print('\nmanycols_1cf')
    num_cols = 100
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf0'], cfparams)
    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf0:%d' % y, value=random_string(2 ** 5)) for y in xrange(num_cols)])

    for x in ['cf0:0', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


def manycols_manycf(client, max_rows, cfparams):
    print('\nmanycols_manycf')
    num_cols = 100
    remove_table(client, 'benchtable')
    with timer('createTable'):
        create_table('benchtable', ['cf%d' % x for x in xrange(num_cols)], cfparams)
    with timer('mutateRow:Create'):
        for x in xrange(max_rows):
            client.mutateRow('benchtable', str(x), [Mutation(column='cf%d:%d' % (y, y), value=random_string(2 ** 5)) for y in xrange(num_cols)])

    for x in ['cf0:0', '']:
        scanner(client, 'benchtable', x, 10, max_rows)
        scanner(client, 'benchtable', x, 1, max_rows)

    delete_rows(client, 'benchtable', max_rows)
    scanner(client, 'benchtable', '', 1, max_rows)
    remove_table(client, 'benchtable')


if __name__ == '__main__':
    client = hadoopy_hbase.connect('localhost')
    cfs = []
    for n, block_size in enumerate([65536 / 16, 65536 / 2, 65536, 65536 * 2, 65536 * 2]):
        cfs.append({'BLOCKSIZE': block_size, 'NAME': 'cf%d' % n})
    create_table('usertable', cfs)
    quit()
    #simple_valsize(client, 10000, cfparams, value_size)
    #ts = get_clear_timer()
    #print((block_size, value_size))
    #print(ts)
    #simple(client, 1000, cfparams)
    #small_large_1cf(client, 100, cfparams)
    #small_large_2cf(client, 100, cfparams)
    #few_many_1cf(client, 10000, cfparams)
    #few_many_2cf(client, 10000, cfparams)
    #manycols_1cf(client, 100, cfparams)
    #manycols_manycf(client, 100, cfparams)
