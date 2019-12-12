
import sys
import os
import win32serviceutil
import win32service
import win32event
import servicemanager
import socket
import cx_Oracle
import logging

# ------------------------------------------------------------------------
class AppServerSvc (win32serviceutil.ServiceFramework):
    _svc_name_ = "dbWatchService"
    _svc_display_name_ = "dbWatch Service"

    # --------------------------------------------------------------------
    def __init__(self,args):

        self.app_dir = "C:\\dbWatch"
        self.timeout = 60 * 60 * 1000
        self.db_dsn = 'tudvl.ce.rt.ru:1521/TUDVL'
        self.db_login = 'gs_api'
        self.db_pass = 'devel'
        # self.oss_service_name = "W3SVC"
        self.oss_service_name = "OSS Service"
        self.oss_machine_name = "S07MPZ01"
        self.address_service_name = "Address Service"
        self.address_machine_name = "S07MPZ01"

        win32serviceutil.ServiceFramework.__init__(self,args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        # logger.setLevel(logging.INFO)

        try:
           handler = logging.FileHandler(self.app_dir + "\\dbwatch.log")
        except PermissionError as e:
           print("logger_init: Error = ", str(e))
           handler = logging.StreamHandler()
           print("logger_init: StreamHandler selected.")
        except Exception as e:
           print("logger_init: Unexpected Error = ", str(e))
           handler = logging.StreamHandler()

        handler.setFormatter(logging.Formatter("%(levelname)s: %(asctime)s - %(process)s - %(message)s"))
        logger.addHandler(handler)

        self.logger = logger


    # --------------------------------------------------------------------
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    # --------------------------------------------------------------------
    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)

        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )

        self.main()

        # win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

    # --------------------------------------------------------------------
    def service_running(self, service, machine):
        return win32serviceutil.QueryServiceStatus(service, machine)[1] == 4

    # --------------------------------------------------------------------
    def stop_service(self, service, machine):
        try:
            win32serviceutil.StopService(service, machine)
        except Exception as e:
            self.logger.error("stop_service: " + str(e))

    # --------------------------------------------------------------------
    def testDB(self):

        try:
            db = cx_Oracle.connect(self.db_login, self.db_pass, self.db_dsn)
            cursor = db.cursor()
            q = cursor.execute(" SELECT  sysdate  FROM  dual ")
            for r in q: pass
            q.close()
            db.close()
            return True, None, r[0]

        except cx_Oracle.DatabaseError as e:
            return False, str(e), None

        except Exception as e:
            return False, str(e), None

    # --------------------------------------------------------------------
    def main(self):
        rc = None

        while rc != win32event.WAIT_OBJECT_0:

            oss_svc_status = self.service_running(self.oss_service_name, self.oss_machine_name)
            addr_svc_status = self.service_running(self.address_service_name, self.address_machine_name)

            self.logger.info('dbWatch Service: OSS service status = ' + str(oss_svc_status))
            self.logger.info('dbWatch Service: Address service status = ' + str(addr_svc_status))

            if oss_svc_status or addr_svc_status:
                result, err, data = self.testDB()
                if err:  self.logger.error('dbWatch Service: Oracle error = ' + str(err))

                if err and ("ORA-01034" in err or
                            "ORA-12514" in err or
                            "ORA-01033" in err or
                            "ORA-01017" in err or
                            "ORA-03135" in err):
                    if oss_svc_status:  self.stop_service(self.oss_service_name, self.oss_machine_name)
                    if addr_svc_status:  self.stop_service(self.address_service_name, self.address_machine_name)
                    os.system("C:\\Perl64\\bin\\perl " + self.app_dir + "\\send_xmpp_msg.pl " + "\"dbWatch: Oracle DB TUDVL." + str(err) + ". Services stopped.\"")
            else:
               if not oss_svc_status:
                   # os.system("C:\\Perl64\\bin\\perl " + self.app_dir + "\\send_xmpp_msg.pl " + "\"dbWatch Service: OSS service stopped\"")
                   pass

               if not addr_svc_status:
                   # os.system("C:\\Perl64\\bin\\perl " + self.app_dir + "\\send_xmpp_msg.pl " + "\"dbWatch Service: Address service stopped\"")
                   pass

            rc = win32event.WaitForSingleObject(self.hWaitStop, self.timeout)

        self.logger('shut down')


# ------------------------------------------------------------------------
if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(AppServerSvc)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(AppServerSvc)
