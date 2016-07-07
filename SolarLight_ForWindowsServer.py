# -*- coding: utf-8 -*-
# This program is used for building the bridge between GSM from Solar Light and users' app
#
#

import socket
import time
import threading
import queue
import pymysql
import pymysql.cursors
import multiprocessing
import base64
import sys



class TCPConnectioin:
    def __init__(self, port):
        self.port = port
        self.server_handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_handle.bind(('0.0.0.0',port))
        self.server_handle.listen(10)


    def connectionKeep(self):
        socket_info = self.server_handle.accept()
        return socket_info


    def connectionClose(self,socket_info):
        sock = socket_info[0]
        address = socket_info[1]

        sock.close()
        #self.server_handle.close()


    def receiveData(self,socket_info):
        sock = socket_info[0]
        address = socket_info[1]
        
        data = sock.recv(1024)
        if not data:
            print('No data received yet!')
            raise ValueError('Received Error!!!')
        else:
            #print()
            print('The data are received from ',address)
            #data = str(data, encoding = 'gbk')
            #print(data)
            #print(int(data[1:]))
            print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            return data


    def sendData(self,data_bytes,socket_info):
        sock = socket_info[0]
        address = socket_info[1]

        print('Sending the data to the ',address)
        #print(data_bytes)
        try:
            sock.sendall(data_bytes)
        except:
            print('The processing of sending data occured error!')
            sock.close()



class DBBase:
    def __init__(self,database_name):
        try:
            with open('dbpasswd','rb') as f_handler:
                db_passwd = str(base64.decodestring(f_handler.read()),encoding = 'utf-8')
        except:
            print('The password file is not found...')
            sys.exit(1)

        db_config = {
                'host':'localhost',
                'port':3306,
                'user':'localusers',
                'password':db_passwd,
                'db':database_name,
                'charset':'utf8mb4',
                'cursorclass':pymysql.cursors.DictCursor,
                }
        self.connection = pymysql.connect(**db_config)


    def dbExec(self,sql,data=None):
        if data == None:
            cursor = self.connection.cursor()
            result = cursor.execute(sql)
            self.connection.commit()
            result = cursor.fetchall()
            cursor.close()
            return result
        else:
            cursor = self.connection.cursor()
            result = cursor.execute(sql,data)
            self.connection.commit()
            result = cursor.fetchall()
            cursor.close()
            return result

        #self.connection.commit()


    def dbClose(self):
        self.connection.close()



class PackageTransfrom:    
    def __bytes2str(self,data_bytes):
        data_str = ''
        for byte in data_bytes:
            data_str = data_str + hex(byte)[2:].zfill(2)

        return data_str


    def __str2bytes(self,data_str):
        data_bytes = b''
        data_temp_str = data_str
        counter = 0
        bytes_list = []

        if len(data_temp_str)%2 == 1:
            data_temp_str = '0' + data_temp_str
        while(counter < len(data_temp_str)/2):
            bytes_list.append(int(data_temp_str[2*counter:2*(counter+1)],16))
            counter = counter + 1
        return bytes(bytes_list)


    def __preProcessPackage(self,package_raw):                                     #为了解决多个数据包被拼接成一个数据包而造成解码失败的情况
        #print(package_raw)
        data,gsm_info,counter = package_raw

        if b'\x81\x85\x86' in data:                                                 #为了和云博的协议保持兼容，节点端已经实现了两套协议，需要在这里加入判断保持完美兼容
            buff = data.split(b'\x81\x85\x86')
            data_return = b'\x81\x85\x86' + buff[1]

        elif b'IDR' in data:
            buff = data.split(b'IDR')
            data_return = b'IDR' + buff[1]

        elif b'STU' in data:
            buff = data.split(b'STU')
            data_return = b'STU' + buff[1]

        elif b'PAR' in data:
            buff = data.split(b'PAR')
            data_return = b'PAR' + buff[1]

        elif b'RSP' in data:
            buff = data.split(b'RSP')
            data_return = b'RSP' + buff[1]

        elif b'BTY' in data:
            buff = data.split(b'BTY')
            data_return = b'BTY' + buff[1]

        elif b'GSM' in data:
            buff = data.split(b'GSM')
            data_return = b'GSM' + buff[1]

        else:
            data_return = data
        return (data_return,gsm_info,counter)


    def processPackageFromGSM(self,data):
        #db_handle = DBBase('SolarLight')
        gsm_id = ''
        gsm_port = ''
        node_id_str = ''
        node_tiny_id_str = ''
        sql = ''
        
        try:
            data = self.__preProcessPackage(data)
            #路灯端返回数据包
            if data[0][0:3] == '\x81\x85\x86':                          #为了和云博的协议保持兼容，增加一处判断，相当与IDR指令的变体
                print(data)
                node_tiny_id_str = self.__bytes2str(data[0][3:5])
                node_id_str = self.__bytes2str(data[0][5:])
                gsm_ip = data[1][0]
                gsm_port = str(data[1][1])
                sql = "SELECT * FROM NodeMapping WHERE Node_ID = %s;"
                data = (node_id_str,)
                db_handle = DBBase('SolarLight')
                res = db_handle.dbExec(sql,data)
                db_handle.dbClose()
                if (res == tuple()) or (res == []):
                    sql = "INSERT NodeMapping (Node_ID,Tiny_ID,GSM_IP,GSM_Port,online) VALUES (%s,%s,%s,%s,%s)"
                    data = (node_id_str,node_tiny_id_str,gsm_ip,gsm_port,1)
                    db_handle = DBBase('SolarLight')
                    try:
                        db_handle.dbExec(sql,data)
                    except:
                        print('We have the problem! IntegrityError:')
                    db_handle.dbClose()
                else:
                    sql = "UPDATE NodeMapping SET Tiny_ID=%s,GSM_IP=%s,GSM_Port=%s,online=%s WHERE Node_ID=%s"
                    data = (node_tiny_id_str,gsm_ip,gsm_port,1,node_id_str)
                    db_handle = DBBase('SolarLight')
                    db_handle.dbExec(sql,data)
                    db_handle.dbClose()

            elif str(data[0][0:3],encoding='utf-8') == 'IDR':           #解码ID命令的返回数据包
                print(data)
                node_tiny_id_str = self.__bytes2str(data[0][4:6])
                node_id_str = self.__bytes2str(data[0][6:])
                gsm_ip = data[1][0]
                gsm_port = str(data[1][1])
                sql = "SELECT * FROM NodeMapping WHERE Node_ID = %s;"
                data = (node_id_str,)
                db_handle = DBBase('SolarLight')
                res = db_handle.dbExec(sql,data)
                db_handle.dbClose()
                #print(res)
                if (res == tuple()) or (res == []):
                    sql = "INSERT NodeMapping (Node_ID,Tiny_ID,GSM_IP,GSM_Port,online) VALUES (%s,%s,%s,%s,%s)"
                    data = (node_id_str,node_tiny_id_str,gsm_ip,gsm_port,1)
                    db_handle = DBBase('SolarLight')
                    try:
                        db_handle.dbExec(sql,data)
                    except:
                        print('We have the problem! IntegrityError:')
                    db_handle.dbClose()
                else:
                    sql = "UPDATE NodeMapping SET Tiny_ID=%s,GSM_IP=%s,GSM_Port=%s,online=%s WHERE Node_ID=%s"
                    data = (node_tiny_id_str,gsm_ip,gsm_port,1,node_id_str)
                    db_handle = DBBase('SolarLight')
                    db_handle.dbExec(sql,data)
                    db_handle.dbClose()

            elif str(data[0][0:3],encoding='utf-8') == 'STU':
                print(data)
                gsm_info = data[1]
                node_tiny_id = self.__bytes2str(data[0][4:6])
                node_id = self.__bytes2str(data[0][6:14])
                node_status_raw_str = str(data[0][14:],encoding='utf-8')
                node_status_raw_list = node_status_raw_str.split("#")
                node_status_list = []
                counter = 0
                del node_status_raw_list[0]
                while(counter < len(node_status_raw_list)):
                    if counter < 5:
                        node_status_list.append(str(float(node_status_raw_list[counter])))
                    else:
                        node_status_list.append(str(int(node_status_raw_list[counter])))
                    counter += 1
                sql = "INSERT NodeStatus (Time,Node_ID,SolarVoltage,CapVoltage,LedVoltage,LedCurrent,PWM,StrongTime_MIN,HalfTime_MIN,WeakTime_MIN,DawnTime_MIN,DigitalResistance_Value,SoftwareTriggerOvervoltage_Flag) VALUES ((SELECT now()),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                data = (node_id,node_status_list[0],node_status_list[1],node_status_list[2],node_status_list[3],node_status_list[4],node_status_list[5],node_status_list[6],node_status_list[7],node_status_list[8],node_status_list[9],node_status_list[10])
                try:
                    db_handle = DBBase('SolarLight')
                    db_handle.dbExec(sql,data)
                    db_handle.dbClose()
                except:
                    print('Insert the data error!')

            elif str(data[0][0:3],encoding='utf-8') == 'PAR':
                print(data)
                gsm_info = data[1]
                node_tiny_id = self.__bytes2str(data[0][4:6])
                node_id = self.__bytes2str(data[0][6:14])
                node_para_raw_str = str(data[0][14:],encoding='utf-8')
                node_para_raw_list = node_para_raw_str.split('#')
                node_para_list = []
                counter = 0
                del node_para_raw_list[0]
                #规范化列表的每一项
                while(counter < len(node_para_raw_list)):
                    if counter > 7 and counter < 14:
                        node_para_list.append(str(int(node_para_raw_list[counter])))
                    else:
                        node_para_list.append(str(int(node_para_raw_list[counter])))
                    counter += 1
                sql = "UPDATE ParameterSetting SET Strong_Power=%s,Half_Power=%s,Weak_Power=%s,Strong_Time=%s,Half_Time=%s,Weak_Time=%s,Dawn_Time=%s,Ready_Time=%s,ThresholdVoltage_Night=%s,ThresholdVoltage=%s,StrongStopVoltage=%s,HalfStopVoltage=%s,SupCap_Enough=%s,SupCap_Discharge_Close=%s,ChargeControl_Dly=%s,SingleCharge_Dly=%s,DynamicParaFlag=%s WHERE Node_ID=%s;"
                data = (node_para_list[0],node_para_list[1],node_para_list[2],node_para_list[3],node_para_list[4],node_para_list[5],node_para_list[6],node_para_list[7],node_para_list[8],node_para_list[9],node_para_list[10],node_para_list[11],node_para_list[12],node_para_list[13],node_para_list[14],node_para_list[15],node_para_list[16],node_id)
                db_handle = DBBase('SolarLight')
                db_handle.dbExec(sql,data)
                db_handle.dbClose()

            elif str(data[0][0:3],encoding='utf-8') == 'RSP':
                print(data)
                para_table_array = (['SSP','Strong_Power'],['SHP','Half_Power'],['SWP','Weak_Power'],['SRT','Ready_Time'],['SST','Strong_Time'],['SHT','Half_Time'],['SWT','Weak_Time'],['SDT','Dawn_Time'],['STN','ThresholdVoltage_Night'],['STV','ThresholdVoltage'],['SSV','StrongStopVoltage'],['SHV','HalfStopVoltage'],['SDC','SupCap_Discharge_Close'],['SSE','SupCap_Enough'],['SCD','ChargeControl_Dly'],['SSD','SingleCharge_Dly'],['SDF','DynamicParaFlag'])
                gsm_info = data[1]
                node_tiny_id = self.__bytes2str(data[0][4:6])
                node_id = self.__bytes2str(data[0][6:14])
                node_para_return_str = str(data[0][14:],encoding='utf-8')
                db_handle = DBBase('SolarLight')
                for para_index in para_table_array:
                    if node_para_return_str == para_index[0]:
                        sql = "UPDATE ParameterSetting SET " + para_index[1] + "=(SELECT "+ para_index[1]+" FROM ParameterCache WHERE Node_ID=%s) WHERE Node_ID=%s;"
                        data = (node_id,node_id)
                        db_handle.dbExec(sql,data)
                
                para_cache_data = db_handle.dbExec("SELECT * FROM ParameterCache WHERE Node_ID=%s;",(node_id,))
                para_set_data = db_handle.dbExec("SELECT * FROM ParameterSetting WHERE Node_ID=%s;",(node_id,))
                db_handle.dbClose()
                para_flag = 0
                for para_index in para_table_array:                                                             #此处用于判断参数是否设置完毕。
                    if para_cache_data[0][para_index[1]] != None:                                               #如果参数设置表中的项目为None，说明该参数从未设置,跳过
                        if para_cache_data[0][para_index[1]] != para_set_data[0][para_index[1]]:
                            para_flag = 1
                            break

                if para_flag == 1:
                    sql = "UPDATE ParameterCache SET status=1 WHERE Node_ID=%s;"
                    data = (node_id,)
                    db_handle = DBBase('SolarLight')
                    db_handle.dbExec(sql,data)
                    db_handle.dbClose()
                else:
                    sql = "UPDATE ParameterCache SET status=0 WHERE Node_ID=%s;"
                    data = (node_id,)
                    db_handle = DBBase('SolarLight')
                    db_handle.dbExec(sql,data)
                    db_handle.dbClose()
            
            elif str(data[0][0:3],encoding='utf-8') == 'BTY':
                print(data)
                gsm_info = data[1]
                gsm_vol = str(data[0][3:],encoding='utf-8')
                sql = "UPDATE NodeMapping SET GSM_Voltage=%s WHERE (GSM_IP=%s AND GSM_Port=%s);"
                data = (gsm_vol,gsm_info[0],str(gsm_info[1]))
                db_handle = DBBase('SolarLight')
                db_handle.dbExec(sql,data)
                db_handle.dbClose()

            elif str(data[0][0:3],encoding='utf-8') == 'GSM':
                db_handle = DBBase('SolarLight')
                gsm_id = self.__bytes2str(data[0][3:11])
                gsm_info = data[1]
                db_handle = DBBase('SolarLight')
                db_handle.dbExec("UPDATE NodeMapping SET GSM_ID=%s,GSM_IP=%s,GSM_Port=%s WHERE (GSM_IP=%s AND GSM_Port=%s) OR (GSM_ID=%s)",(gsm_id,gsm_info[0],str(gsm_info[1]),gsm_info[0],str(gsm_info[1]),gsm_id))
                db_handle.dbClose()
            else:
                print(data)
                print('Invaild package!')
        except UnicodeDecodeError as e:
            print('We have a problem! UnicodeDecodeError:',e)
        except:
            print('We have a problem, but we must continue!')


    def sendInquireParameterToNodes(self,send_package_queue):
        print('Sending packages of getting the parameters from every node...')
        db_handle = DBBase('SolarLight')
        nodes_info_list = db_handle.dbExec("SELECT Tiny_ID,GSM_IP,GSM_Port FROM NodeMapping WHERE online=1;")
        db_handle.dbClose()
        for node_info_dic in nodes_info_list:
            sending_package = b'CK' + self.__str2bytes(node_info_dic['Tiny_ID']) + b'VPM'
            gsm_info = (node_info_dic['GSM_IP'],int(node_info_dic['GSM_Port']))
            send_package_queue.put((sending_package,gsm_info,1))
            send_package_queue.put((sending_package,gsm_info,1))                                    #将数据压入队列两次，也就会发送两次，提高成功率


    def sendInquireStatusToNodes(self,send_package_queue):
        print('Sending packages of inquiring status...')
        db_handle = DBBase('SolarLight')
        node_tiny_id_list = db_handle.dbExec("SELECT Tiny_ID,GSM_IP,GSM_Port FROM NodeMapping WHERE online=1;")
        db_handle.dbClose()
        for node_tiny_id_dic in node_tiny_id_list:
            try:
                sending_package = b'CK'+self.__str2bytes(node_tiny_id_dic['Tiny_ID'])+b'VS'
                gsm_info = (node_tiny_id_dic['GSM_IP'],int(node_tiny_id_dic['GSM_Port']))
                send_package_queue.put((sending_package,gsm_info,1))
                send_package_queue.put((sending_package,gsm_info,1))                                #将数据压入队列两次，也就会发送两次，提高成功率
            except:
                print('We have a problem when sending packages of inquiring status...')


    def sendInquireGSMVolatage(self,send_package_queue):
        print('Sending packages of getting voltages of GSMs...')
        db_handle = DBBase('SolarLight')
        gsms_info_list = db_handle.dbExec("SELECT DISTINCT GSM_ID,GSM_IP,GSM_Port FROM NodeMapping WHERE online=1;")
        db_handle.dbClose()
        for gsm_info_dic in gsms_info_list:
            sending_package = b'VGB'
            gsm_info = (gsm_info_dic['GSM_IP'],int(gsm_info_dic['GSM_Port']))
            send_package_queue.put((sending_package,gsm_info,1))


    def sendSettingParameterToNodes(self,send_package_queue):
        print('Sending packages of setting parameters...')
        db_handle = DBBase('SolarLight')
        nodes_info_list = db_handle.dbExec("SELECT Node_ID,Tiny_ID,GSM_ID,GSM_IP,GSM_Port FROM NodeMapping WHERE online=1;")
        db_handle.dbClose()
        for node_info_dic in nodes_info_list:
            db_handle = DBBase('SolarLight')
            node_para_cache_list = db_handle.dbExec("SELECT * FROM ParameterCache WHERE Node_ID=%s;",node_info_dic['Node_ID'])
            db_handle.dbClose()
            #print(node_para_cache_list)
            if node_para_cache_list[0]['status'] == b'\x01':
                db_handle = DBBase('SolarLight')
                node_para_set_list = db_handle.dbExec("SELECT * FROM ParameterSetting WHERE Node_ID=%s",node_info_dic['Node_ID'])
                db_handle.dbClose()
                gsm_info = (node_info_dic['GSM_IP'],int(node_info_dic['GSM_Port']))
                
                if node_para_cache_list[0]['Strong_Power'] != None:
                    if node_para_cache_list[0]['Strong_Power'] != node_para_set_list[0]['Strong_Power']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SSP'+bytes(node_para_cache_list[0]['Strong_Power'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))            #压入队列两次保证成功率
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Half_Power'] != None:
                    if node_para_cache_list[0]['Half_Power'] != node_para_set_list[0]['Half_Power']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SHP'+bytes(node_para_cache_list[0]['Half_Power'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Weak_Power'] != None:
                    if node_para_cache_list[0]['Weak_Power'] != node_para_set_list[0]['Weak_Power']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SWP'+bytes(node_para_cache_list[0]['Weak_Power'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Ready_Time'] != None:
                    if node_para_cache_list[0]['Ready_Time'] != node_para_set_list[0]['Ready_Time']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SRT'+bytes(node_para_cache_list[0]['Ready_Time'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Strong_Time'] != None:
                    if node_para_cache_list[0]['Strong_Time'] != node_para_set_list[0]['Strong_Time']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SST'+bytes(node_para_cache_list[0]['Strong_Time'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Half_Time'] != None:
                    if node_para_cache_list[0]['Half_Time'] != node_para_set_list[0]['Half_Time']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SHT'+bytes(node_para_cache_list[0]['Half_Time'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Weak_Time'] != None:
                    if node_para_cache_list[0]['Weak_Time'] != node_para_set_list[0]['Weak_Time']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SWT'+bytes(node_para_cache_list[0]['Weak_Time'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['Dawn_Time'] != None:
                    if node_para_cache_list[0]['Dawn_Time'] != node_para_set_list[0]['Dawn_Time']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SDT'+bytes(node_para_cache_list[0]['Dawn_Time'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['ThresholdVoltage_Night'] != None:
                    if node_para_cache_list[0]['ThresholdVoltage_Night'] != node_para_set_list[0]['ThresholdVoltage_Night']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'STN'+bytes(node_para_cache_list[0]['ThresholdVoltage_Night'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['ThresholdVoltage'] != None:
                    if node_para_cache_list[0]['ThresholdVoltage'] != node_para_set_list[0]['ThresholdVoltage']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'STV'+bytes(node_para_cache_list[0]['ThresholdVoltage'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['StrongStopVoltage'] != None:
                    if node_para_cache_list[0]['StrongStopVoltage'] != node_para_set_list[0]['StrongStopVoltage']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SSV'+bytes(node_para_cache_list[0]['StrongStopVoltage'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['HalfStopVoltage'] != None:
                    if node_para_cache_list[0]['HalfStopVoltage'] != node_para_set_list[0]['HalfStopVoltage']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SHV'+bytes(node_para_cache_list[0]['HalfStopVoltage'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['SupCap_Discharge_Close'] != None:
                    if node_para_cache_list[0]['SupCap_Discharge_Close'] != node_para_set_list[0]['SupCap_Discharge_Close']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SDC'+bytes(node_para_cache_list[0]['SupCap_Discharge_Close'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['SupCap_Enough'] != None:
                    if node_para_cache_list[0]['SupCap_Enough'] != node_para_set_list[0]['SupCap_Enough']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SSE'+bytes(node_para_cache_list[0]['SupCap_Enough'].zfill(3),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['ChargeControl_Dly'] != None:
                    if node_para_cache_list[0]['ChargeControl_Dly'] != node_para_set_list[0]['ChargeControl_Dly']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SCD'+bytes(node_para_cache_list[0]['ChargeControl_Dly'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['SingleCharge_Dly'] != None:
                    if node_para_cache_list[0]['SingleCharge_Dly'] != node_para_set_list[0]['SingleCharge_Dly']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SSD'+bytes(node_para_cache_list[0]['SingleCharge_Dly'].zfill(2),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))

                if node_para_cache_list[0]['DynamicParaFlag'] != None:
                    if node_para_cache_list[0]['DynamicParaFlag'] != node_para_set_list[0]['DynamicParaFlag']:
                        sending_package = b'CK'+self.__str2bytes(node_info_dic['Tiny_ID'])+b'SDF'+bytes(node_para_cache_list[0]['DynamicParaFlag'].zfill(1),encoding='utf-8')
                        send_package_queue.put((sending_package,gsm_info,1))
                        send_package_queue.put((sending_package,gsm_info,1))
            else:
                continue


    def sendSearchIDPackage(self,send_package_queue):
        print('Sending ID packages...')
        db_handle = DBBase('SolarLight')
        db_handle.dbExec("UPDATE NodeMapping SET online=0")
        node_mapping_data = db_handle.dbExec('SELECT Node_ID,GSM_IP,GSM_Port FROM NodeMapping;')
        db_handle.dbClose()
        #print(node_mapping_data)
        for node_mapping_dic in node_mapping_data:
            sending_package = b'ID'+self.__str2bytes(node_mapping_dic['Node_ID'])
            if (node_mapping_dic['GSM_IP'] == None) or (node_mapping_dic['GSM_Port'] == None):
                continue
            else:
                gsm_info = (node_mapping_dic['GSM_IP'],int(node_mapping_dic['GSM_Port']))
                send_package_queue.put((sending_package,gsm_info,1))


    def processPackageFromAppAndResponse(self,data):
        try:
            pass
            #app端命令解析
            if str(data[0][0:3],encoding='utf-8') == 'CKN':          #通过节点编号check指令
                pass
            elif str(data[0][0:3],encoding='utf-8') == 'CKA':        #check多个节点指令
                pass
            elif str(data[0][0:3],encoding='utf-8') == 'STA':        #设置多个节点参数指令
                pass
            elif str(data[0][0:3],encoding='utf-8') == 'NWA':        #查看当前在线路灯总数和编号
                pass

            elif str(data[0][0:3],encoding='utf-8') == "LOC":       #app发送的路灯位置数据
                print('Received location of light....')
                print(data)
                node_id_str = ''
                app_info = data[1]
                try:                                                        #当扫码不正确时的异常处理
                    node_id_raw_str = str(data[0][3:26],encoding='utf-8')
                    node_id_part_list = node_id_raw_str.split(' ')
                    for part_index in node_id_part_list:
                        node_id_str = node_id_str + part_index
                    data_raw = str(data[0][26:],encoding='utf-8')
                    data_raw_list = data_raw.split('#')
                    del data_raw_list[0]
                    longitude = data_raw_list[0]                            #获取经度
                    latitude = data_raw_list[1]                             #获取纬度

                    sql = "SELECT * FROM NodeMapping WHERE Node_ID = %s;"
                    data = (node_id_str,)
                    db_handle = DBBase("SolarLight")
                    res = db_handle.dbExec(sql,data)
                    db_handle.dbClose()
                    if (res == tuple()) or (res == []):
                        sql = "INSERT NodeMapping (Node_ID,Latitude,Longitude,online) VALUES (%s,%s,%s,%s);"
                        data = (node_id_str,latitude,longitude,0)
                        db_handle = DBBase('SolarLight')
                        try:
                            db_handle.dbExec(sql,data)
                            db_handle.dbClose()
                        except pymysql.err.IntegrityError as e:
                            print('We have the problem! IntegrityError:',e)
                            db_handle.dbClose()
                            return (b'LOC1',app_info,1)
                        return (b'LOC0',app_info,1)
                    else:
                        sql = "UPDATE NodeMapping SET Latitude=%s,Longitude=%s WHERE Node_ID=%s"
                        data = (latitude,longitude,node_id_str)
                        db_handle = DBBase('SolarLight')
                        db_handle.dbExec(sql,data)
                        db_handle.dbClose()
                        return (b'LOC0',app_info,1)
                except:
                    print("Scan Error Code!!")
                    return (b'LOC1',app_info,1)

        except UnicodeDecodeError as e:
            print('We have a problem! UnicodeDecodeError:',e)
        except:
            print('We have a problem, but we must continue!')



class DBConnection(multiprocessing.Process):
    def __init__(self, package_send_to_GSM_queue, package_receive_from_GSM_queue, package_receive_from_app_queue, package_send_to_app_queue):
        multiprocessing.Process.__init__(self)
        self.package_send_to_GSM_queue = package_send_to_GSM_queue
        self.package_receive_from_GSM_queue = package_receive_from_GSM_queue
        
        self.package_receive_from_app_queue = package_receive_from_app_queue
        self.package_send_to_app_queue = package_send_to_app_queue


    def __sendPackagesToGSMs(self):
        package_transfrom_handle = PackageTransfrom()
        last_run_search_id_time = 0
        last_run_set_para_time = 0
        last_run_inquire_status_time = 0
        last_run_inquire_para_time = 0
        last_run_inquire_gsm_voltage_time = 0
        time.sleep(43)                                                                      #等待收到启动后的第一个GSM心跳包，保证第一次发送命令（尤其是ID命令）可以成功
        
        while True:
            if time.time()-last_run_search_id_time > 31*60:
                package_transfrom_handle.sendSearchIDPackage(self.package_send_to_GSM_queue)
                last_run_search_id_time = time.time()
                continue
            
            elif time.time() - last_run_set_para_time > 5*60:
                package_transfrom_handle.sendSettingParameterToNodes(self.package_send_to_GSM_queue)
                last_run_set_para_time = time.time()
                continue
            
            elif time.time() -last_run_inquire_status_time > 17*60:
                package_transfrom_handle.sendInquireStatusToNodes(self.package_send_to_GSM_queue)
                last_run_inquire_status_time = time.time()
                continue
            
            elif time.time() - last_run_inquire_para_time > 61*60:
                package_transfrom_handle.sendInquireParameterToNodes(self.package_send_to_GSM_queue)
                last_run_inquire_para_time = time.time()
                continue

            elif time.time() - last_run_inquire_gsm_voltage_time > 23*60:
                package_transfrom_handle.sendInquireGSMVolatage(self.package_send_to_GSM_queue)
                last_run_inquire_gsm_voltage_time = time.time()
                continue
            
            else:
                time.sleep(13)
                continue


    def __receivePackagesFromGSMs(self):
        package_transfrom_handle = PackageTransfrom() 
        while True:
            package_receive_from_GSM = self.package_receive_from_GSM_queue.get()
            package_transfrom_handle.processPackageFromGSM(package_receive_from_GSM)


    def __receivePackagesFromApps(self):
        package_transfrom_handle = PackageTransfrom()
        while True:
            package_receive_from_app = self.package_receive_from_app_queue.get()
            response_packages_to_app = package_transfrom_handle.processPackageFromAppAndResponse(package_receive_from_app)
            self.package_send_to_app_queue.put(response_packages_to_app)


    def run(self):
        submit_packagesToGSM = threading.Thread(target=self.__sendPackagesToGSMs)
        inquire_packagesFromGSM = threading.Thread(target=self.__receivePackagesFromGSMs)

        process_packagesFromApp = threading.Thread(target=self.__receivePackagesFromApps)

        submit_packagesToGSM.start()
        inquire_packagesFromGSM.start()
        process_packagesFromApp.start()
        submit_packagesToGSM.join()
        inquire_packagesFromGSM.join()
        process_packagesFromApp.join()



class GSMConnection(multiprocessing.Process):
    def __init__(self, receive_package_queue, send_package_queue, connection_port):
        multiprocessing.Process.__init__(self)
        self.receive_package_queue = receive_package_queue
        self.send_package_queue = send_package_queue
        self.tcp_handle = TCPConnectioin(connection_port)
        self.connection_port = connection_port


    def __receiveDataFromGSM(self,socket_info):
        while True:
            print('Current index code of this process:' + str(self.connection_port))
            try:
                package = self.tcp_handle.receiveData(socket_info)
            except socket.error as e:
                #self.tcp_handle.connectionClose(socket_info)
                print('We have a problem! Maybe receiving data timeout! Info:',e)
                break
            except ValueError as e:
                #self.tcp_handle.connectionClose(socket_info)
                print('We have a program! Received Error package! Info: ',e)
                break
            recevie_data = (package,socket_info[1],0)
            self.receive_package_queue.put(recevie_data)


    def __sendDataFromGSM(self,socket_info):
        fail_counter = 0
        start_time = time.time()

        while True:
            #print('start_time:',start_time)
            #print('fail_counter:',fail_counter)
            #print('info:',socket_info[1])
            if not self.send_package_queue.empty():
                package = self.send_package_queue.get()
                print(package)
                gsm_ip_str = package[1][0]
                gsm_port_int = package[1][1]
                if (gsm_ip_str == socket_info[1][0]) and (gsm_port_int == socket_info[1][1]):
                    self.tcp_handle.sendData(package[0],socket_info)
                    fail_counter = 0
                    time.sleep(2)
                else:
                    if package[2] < 3:                                                              #重试的次数等于当前发送线程的数量再加三
                        data,con_info,counter = package
                        counter = counter + 1
                        package = (data,con_info,counter)
                        self.send_package_queue.put(package)
                        time.sleep(2)
                    else:
                        fail_counter += 1
                        print('Sending package timeout!')
            #else:
            if fail_counter >= 5:
                print('The thread of the sending data to gsm is closed...')
                self.tcp_handle.connectionClose(socket_info)
                break
            time.sleep(1)


    def run(self):
        while True:
            socket_info = self.tcp_handle.connectionKeep()
            thread_recevie_data = threading.Thread(target=self.__receiveDataFromGSM, args=(socket_info,))
            thread_send_data = threading.Thread(target=self.__sendDataFromGSM,args=(socket_info,))
            thread_recevie_data.start()
            thread_send_data.start()



class ClientConnection(multiprocessing.Process):
    def __init__(self,receive_package_queue,send_package_queue):
        multiprocessing.Process.__init__(self)
        self.receive_package_queue = receive_package_queue
        self.send_package_queue = send_package_queue
        self.tcp_handle = TCPConnectioin(20000)


    def __processDataFromApp(self,socket_info):
        try:
            package = self.tcp_handle.receiveData(socket_info)
        except socket.error as e:
            self.tcp_handle.connectionClose(socket_info)
            print('We have a problem! Maybe receiving data timeout! Info:',e)
            self.tcp_handle.connectionClose(socket_info)
            return
        except ValueError as e:
            self.tcp_handle.connectionClose(socket_info)
            print('We have a problem! Received Error package! Info: ',e)
            self.tcp_handle.connectionClose(socket_info)
            return
        except:
            print('we have a problem! Other problem!')
            self.tcp_handle.connectionClose(socket_info)
            return

        receive_data = (package,socket_info[1],0)
        #print(receive_data)
        self.receive_package_queue.put(receive_data)
        while True:
            if not self.send_package_queue.empty():
                response_to_app = self.send_package_queue.get()
                if (response_to_app[1][0] == socket_info[1][0]) and (response_to_app[1][1] == socket_info[1][1]):
                    self.tcp_handle.sendData(response_to_app[0],socket_info)
                    break
                else:
                    self.send_package_queue.put(response_to_app)
        self.tcp_handle.connectionClose(socket_info)
 

    def run(self):
        while True:
            connection_handle = self.tcp_handle.connectionKeep()
            thread_processing_handle = threading.Thread(target=self.__processDataFromApp,args=(connection_handle,))
            thread_processing_handle.start()



if __name__ == '__main__':
    receive_queue_from_node = multiprocessing.Queue()
    send_queue_to_node = multiprocessing.Queue()
    receive_queue_from_app = multiprocessing.Queue()
    send_queue_to_app = multiprocessing.Queue()

    db_connection_handle = DBConnection(send_queue_to_node,receive_queue_from_node,receive_queue_from_app,send_queue_to_app)
    db_connection_handle.start()
    gsm_xinhua_connection = GSMConnection(receive_queue_from_node,send_queue_to_node,30001)
    gsm_xinhua_connection.start()
    gsm_juzizhou_connection = GSMConnection(receive_queue_from_node,send_queue_to_node,30002)
    gsm_juzizhou_connection.start()
    client_connection = ClientConnection(receive_queue_from_app,send_queue_to_app)
    client_connection.start()
    db_connection_handle.join()
    gsm_xinhua_connection.join()
    gsm_juzizhou_connection.join()
    client_connection.join()

