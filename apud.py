### tool by Kamil Stawiarski (@ora600pl www.ora-600.pl) 
### this is very primitive tool, made only for learning - it makes things easier while analyzing redo
### the tool has been created as support tool for V00D00 - logical replication appliance
import struct, binascii

class DumpParser(object):
    operation_types = {}
    operation_types["OP:11.3"]="DELETE"
    operation_types["OP:11.5"]="UPDATE"
    operation_types["OP:11.2"]="INSERT"
    operation_types["OP:11.11"]="INSERT_ARRAY"
    operation_types["OP:10.2"]="INSERT Leaf Row"
    operation_types["OP:10.4"]="DELETE Leaf Row"
    operation_types["OP:10.3"]="PURGE Leaf Row"
    operation_types["OP:10.5"]="Restore Leaf during rollback"
    operation_types["OP:10.35"]="IOT_UPDATE"
    operation_types["OP:5.1"]="UNDO"
    operation_types["OP:5.4"]="COMMIT"
    operation_types["OP:5.6"]="ROLLBACK SINGLE STATEMENT (not final)"
    operation_types["OP:5.11"]="ROLLBACK SINGLE STATEMENT (final)"
    operation_types["OP:19.1"]="DIRECT PATH INSERT"
    operation_types["OP:26.6"]="LOB"
    operation_types["OP:5.2"]="UNDO HEADER"
    operation_types["OP:4.1"]="Block cleanout record"

    def __init__(self):
        self.redo_records = {}
        self.change_vectors = {}
        self.object_names = {}
        self.registered_xids = []

        self.version = "12c"
        self.offsets = {}
        self.offsets["12c"] = {}
        self.offsets["12c"]["OP_CODE"]=10
        self.offsets["12c"]["OBJD"]=7
        self.offsets["12c"]["CLS"]=4

        self.offsets["11g"] = {}
        self.offsets["11g"]["OP_CODE"]=9
        self.offsets["11g"]["OBJD"]=6
        self.offsets["11g"]["CLS"]=3

    def set_version(self,ver):
        self.version = ver

    def parse_file(self,tracefile_path, oname):
        self.registered_xids = []
        self.redo_records = {}
        self.change_vectors = {}

        tracefile = open(tracefile_path,"r")
        block_number = 0
        offset = 0
        sequence = 0
        rs_id = " "
        xid = " "
        operation_type = " -- undefined -- "
        for line in tracefile:
            if line.startswith("REDO RECORD"):
                sequence = int(line.split()[5].split(".")[0][2:],16)
                block_number = int(line.split()[5].split(".")[1],16)
                offset = int(line.split()[5].split(".")[2],16)
                rs_id = line.split()[5]
                self.redo_records[sequence,block_number,offset,rs_id] = []
                operation = -1
                operation_type = " -- undefined -- "
            elif line.startswith("CHANGE #") and line.find("MEDIA RECOVERY MARKER")<0:
                try:
                    op_code = line.split()[self.offsets[self.version]["OP_CODE"]]
                    object_id = int(line.split()[self.offsets[self.version]["OBJD"]][4:])
                except BaseException as e:
                    print str(e)
                    print "are you trying to parse for version " + self.version + "? If no, change it. " \
                                                                                  "If yes - something is wrong :)"
                    print line 
                    print "offset: " + str(self.offsets[self.version]["OP_CODE"])
                    return 0

                operation_type = DumpParser.operation_types.get(op_code,op_code)
                if operation_type == "COMMIT":
                    xid = line.split()[self.offsets[self.version]["CLS"]][4:].strip()

                object_name = self.object_names.get(object_id,str(object_id))
                self.redo_records[sequence,block_number,offset,rs_id].append([object_name,operation_type,xid])
                operation += 1

            elif (operation_type == "UNDO" or operation_type.startswith("ROLLBACK")
                  or operation_type == "INITIATE TRANSACTION") and line.find("objd: ")>0:

                try:
                    object_id = int(line.split()[-3])
                except:
                    object_id = -1

                object_name = self.object_names.get(object_id,str(object_id).strip())
                change_vectors = len(self.redo_records[sequence,block_number,offset,rs_id])
                self.redo_records[sequence,block_number,offset,rs_id][change_vectors-1][0]=object_name
                if object_name == str(oname).strip():
                    self.registered_xids.append(self.redo_records[sequence,block_number,offset,rs_id][change_vectors-1][2])

            elif operation_type == "UNDO" and len(line.split())>0 and line.split()[0].strip()=="xid:":
                xid = line.split()[1]
                change_vectors = len(self.redo_records[sequence,block_number,offset,rs_id])
                for i in range(change_vectors):
                    self.redo_records[sequence,block_number,offset,rs_id][i][2]=xid

            elif operation_type == "COMMIT" and line.startswith("ktucm"):
                cls = int(self.redo_records[sequence,block_number,offset,rs_id][operation][2])
                xidusn = struct.Struct("2s").unpack(struct.Struct(">H").pack((cls-17)/2+1))[0]
                xidslt = line.split()[3][3:].strip()
                xidsqn = line.split()[5][2:].strip()
                xid = "0x" + binascii.hexlify(xidusn) + "." + xidslt + "." + xidsqn
                self.redo_records[sequence,block_number,offset,rs_id][operation][2] = xid

                flg = int(line.split()[11][2:].strip(),16)
                if flg == 4 or flg == 20:
                    self.redo_records[sequence,block_number,offset,rs_id][operation][1] = "COMMIT [ending rollback]"

        tracefile.close()


    def show_records(self,object_name):
        for k in sorted(self.redo_records.keys()):
            change_no = 0
            record_header = 0
            for i in self.redo_records[k]:
                if i[0] == object_name and (change_no == 0 or record_header == 0):
                    print "\nRedo record sequence: {0} block: {1} offset {2} [rs_id: {3}] ".format(str(k[0]), str(k[1]),
                                                                                                   str(k[2]), str(k[3]))
                    print "\t change {0} operation {1} xid = {2}".format(str(change_no+1),i[1],str(i[2]))
                    record_header = 1
                elif i[0] == object_name and change_no > 0:
                    print "\t change {0} operation {1} xid = {2}".format(str(change_no + 1), i[1], str(i[2]))
                elif i[1].startswith("COMMIT") and i[2] in self.registered_xids:
                    print "{0} for transaction {1} [sequence: {2} block: {3} offset {4} rs_id: {5} ]".format(i[1],i[2],
                                                                                                             str(k[0]),
                                                                                                             str(k[1]),
                                                                                                             str(k[2]),
                                                                                                             str(k[3]))

                change_no += 1


    def connect_to_oracle(self,connect_string):
        try:
            __import__('imp').find_module('cx_Oracle')
            import cx_Oracle 
            con = cx_Oracle.connect(connect_string)
            cur = con.cursor() 
            cur.execute("select data_object_id, owner || '.' || object_name as oname from dba_objects")
            for row in cur:
                self.object_names[row[0]] = row[1]

            cur.close()

            con.close()
        except ImportError:
            print "You have to install cx_Oracle to handle Oracle database. Try #pip install cx_Oracle. " \
                  "And setup ENV properly!"
        except BaseException as e:
            print "Something went wrong: " + str(e)


    @staticmethod
    def help():
        print "parse file /path/to/a/logfile/dump [data_object_id | owner.object_name]   " \
              "if you want to parse file based on DATA_OBJECT_ID or OWNER.OBJECT_NAME"
        print "get dict user/password@ip:port/service                                    " \
              "if you want to use object_name instead of data_object_id\n"

        print "default supported version is 12c, to change to 11g type: set version 11g"

        print "exit to quit"
        print "help to print this"


if __name__=='__main__':
    dp = DumpParser()
    dp.help()
    cnt = True
    while cnt: 
        try:
            command = raw_input("V00D00 > ").strip()
            if command == "exit":
                cnt = False
            elif command.startswith("parse file"):
                dp.parse_file(command.split()[2],command.split()[3])
                dp.show_records(command.split()[3])
            elif command.startswith("get dict"):
                dp.connect_to_oracle(command.split()[2])
            elif command == "set version 11g":
                dp.set_version("11g")
            elif command == "help":
                dp.help()
            elif len(command)>0:
                print "\n\nWhat???"
                dp.help()


        except BaseException as e:
            print "You messed up... Or I messed up. Something is messed up"
            print str(e)
            raise

