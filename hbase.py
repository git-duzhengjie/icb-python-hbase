# coding=utf-8
# Author: ruin
"""
discrible:

"""
import getopt

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
import sys

sys.path.append('./gen-py/hbase')
import Hbase
import struct


# Method for encoding ints with Thrift's string encoding
def encode(n):
    return struct.pack("i", n)


# Method for decoding ints with Thrift's string encoding
def decode(s):
    return int(s) if s.isdigit() else struct.unpack('i', s)[0]


class HBaseApi(object):

    def __init__(self, table='test', host='192.168.137.5', port=9090, column_types=[bytes], column_list=['cl']):
        self.table = table.encode('utf-8')
        self.host = host
        self.port = port
        # Connect to HBase Thrift server
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        self.protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)

        # Create and open the client connection
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        # set type and field of column families
        self.set_column_families(column_types, column_list)
        self._build_column_families()

    def set_column_families(self, type_list, col_list):
        self.columnFamiliesType = type_list
        self.columnFamilies = col_list

    def _build_column_families(self):
        """
        give all column families name list,create a table
        :return:
        """
        tables = self.client.getTableNames()
        if self.table not in tables:
            self.__create_table(self.table)

    def __create_table(self, table):
        """
        create table in hbase with column families
        :param table: fr_test_hbase:fr_test
        :return:
        """

        columnFamilies = []
        for columnFamily in self.columnFamilies:
            name = Hbase.ColumnDescriptor(name=columnFamily)
            columnFamilies.append(name)
        table = table.encode('utf-8')
        print(type(table), type(columnFamilies))
        self.client.createTable(table, columnFamilies)

    def __del__(self):
        self.transport.close()

    def __del_table(self, table):
        """
        delete a table,first need to disable it
        """
        self.client.disableTable(table)
        self.client.deleteTable(table)

    def getColumnDescriptors(self):
        return self.client.getColumnDescriptors(self.table)

    def put(self, rowKey, qualifier, value):
        """
        put one row
        column is column name,value is column value
        :param qualifier:
        :param rowKey: rowKey
        :param column: column name
        :param value: column value
        :description: HbaseApi(table).put('rowKey','column','value')
        """

        rowKey = rowKey.encode('utf-8')
        mutations = []
        # for j, column in enumerate(column):
        m_name = None
        if isinstance(value, str):
            value = value.encode('utf-8')
            m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'), value=value)
        elif isinstance(value, int):
            m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'),
                                    value=encode(value))
        mutations.append(m_name)
        self.client.mutateRow(self.table, rowKey, mutations, {})

    # def puts(self, rowKeys, qualifier, values):
    #     """ put sevel rows, `qualifier` is autoincrement
    #
    #     :param rowKeys: a single rowKey
    #     :param values: values is a 2-dimension list, one piece element is [name, sex, age]
    #     :param qualifier: column family qualifier
    #     """
    #
    #     mutationsBatch = []
    #     if not isinstance(rowKeys, list):
    #         rowKeys = [rowKeys] * len(values)
    #     for i in range(0, n):
    #
    #         for i, value in enumerate(values):
    #             mutations = []
    #             # for j, column in enumerate(value):
    #             m_name = None
    #             if isinstance(value, str):
    #                 value = value.encode('utf-8')
    #                 m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'), value=value)
    #             elif isinstance(value, int):
    #                 m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'),
    #                                         value=encode(value))
    #             mutations.append(m_name)
    #             mutationsBatch.append(Hbase.BatchMutation(row=rowKeys[i].encode('utf-8'), mutations=mutations))
    #         self.client.mutateRows(self.table, mutationsBatch, {})

    def puts(self, qualifier, interval=10000, target_id=0):
        """ put rows, `qualifier` is autoincrement

        :param n put num
        :param interval
        :param qualifier: column family qualifier
        :param threads
        :param target_id current thread id
        Usage::

        """
        num = int(n / threads / interval) + 1
        start = target_id * int(n / threads)
        for i in range(num):
            if i + 1 != num:
                rowKeys = [str(key) for key in range(i * interval + start, (i + 1) * interval + start)]
                values = rowKeys
            else:
                rowKeys = [str(key) for key in range(i * interval + start, i * interval + num % interval + start)]
                values = rowKeys
            mutationsBatch = []
            for j, value in enumerate(values):
                mutations = []
                m_name = None
                if isinstance(value, str):
                    value = value.encode('utf-8')
                    m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'),
                                            value=value)
                elif isinstance(value, int):
                    m_name = Hbase.Mutation(column=(self.columnFamilies[0] + ':' + qualifier).encode('utf-8'),
                                            value=encode(value))
                mutations.append(m_name)
                mutationsBatch.append(Hbase.BatchMutation(row=rowKeys[j].encode('utf-8'), mutations=mutations))
            self.client.mutateRows(self.table, mutationsBatch, {})

    def getRow(self, row, qualifier='name'):
        """
        get one row from hbase table
        :param row:
        :param qualifier:
        :return:
        """
        # res = []
        row = self.client.getRow(self.table, row.encode('utf-8'), {})
        for r in row:
            rd = {}
            row = r.row.decode('utf-8')
            value = r.columns[b'cf:name'].value.decode('utf-8')
            rd[row] = value
            # res.append(rd)
            # print ('the row is ',r.row.decode('utf-8'))
            # print ('the value is ',(r.columns[b'cf:name'].value).decode('utf-8'))
            return rd

    def getRows(self, rows, qualifier='name'):
        """
        get rows from hbase,all the row sqecify the same 'qualifier'
        :param rows: a list of row key
        :param qualifier: column
        :return: None
        """
        # grow = True if len(rows) == 1 else False
        res = []
        for r in rows:
            res.append(self.getRow(r, qualifier))
        return res

    def scanner(self, numRows=100, startRow=None, stopRow=None):
        """

        :param numRows:
        :param startRow:
        :param stopRow:
        :return:
        """
        scan = Hbase.TScan(startRow, stopRow)
        scannerId = self.client.scannerOpenWithScan(self.table, scan, {})

        ret = []
        rowList = self.client.scannerGetList(scannerId, numRows)

        for r in rowList:
            rd = {}
            row = r.row.decode('utf-8')
            # print(r.columns['cf:name'])
            if 'cf:name' not in r.columns:
                continue
            value = r.columns['cf:name'].value.decode('utf-8')
            rd[row] = value
            # print ('the row is ',r.row.decode('utf-8'))
            # print ('the value is ',(r.columns[b'cf:name'].value).decode('utf-8'))
            ret.append(rd)

        return ret


def process(target_id):
    ha = HBaseApi(table='usertable2', host='192.168.0.230', port=9090, column_list=['family'])
    # ha.put('0002','age','23')
    # rowKeys = [str(key) for key in range(0 + n * 100000000, 100000000 + n * 100000000)]
    # values = ['fr' + str(val) for val in range(0 + n * 100000000, 100000000 + n * 100000000)]
    # ha.puts(rowKeys, 'name', values)
    ha.puts('name', target_id=target_id)
    # print(ha.getRow('0001'))
    # print(ha.getRows(rowKeys))


def usage():
    print("Usage:python hbase.py -n record -t threads or \n"
          "      python hbase.py --record=record --threads=threads")
    sys.exit()


def check():
    flag = 0
    try:
        options, args = getopt.getopt(sys.argv[1:], "hn:t:", ["help", "record=", "threads="])
    except getopt.GetoptError:
        sys.exit()
    for name, value in options:
        if name in ("-h", "--help"):
            usage()
        if name in ("-n", "--record"):
            print('record is----', value)
            global n
            n = int(value)
            flag += 1
        if name in ("-t", "--threads"):
            print('threads is----', value)
            global threads
            threads = int(value)
            flag += 1
    if flag != 2:
        usage()


if __name__ == "__main__":
    from multiprocessing import Pool
    threads = None
    n = None
    check()
    pool = Pool(processes=threads)
    x = list(range(threads))
    print(x)
    pool.map(process, x)
