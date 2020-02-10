
import time
import logging

# logs time progress of program
plogger = logging.getLogger('tprogress')
plogger.setLevel(logging.INFO)


class progress :

    def __init__(self,max_num=1,step_size=1) :
        self.inc = -1
        self.step_size = step_size
        self.max = max_num
        self.time_start = time.time()
        self.time_last = self.time_start
        self.time_end_projected = time.time()
        self.strtime_end_projected = time.strftime("%H:%M:%S", time.localtime(self.time_end_projected))
        plogger.debug("Start time: " + time.strftime('%H:%M:%S  %Y-%m-%d '))

    # called within a loop to state past time and projected end time
    def monitor(self):
        self.inc += 1
        if self.inc % self.step_size == 0:
            time_taken = time.time() - self.time_start
            if self.inc != 0 :
                self.time_end_projected = time_taken / (self.inc*self.step_size) * self.max + self.time_start
                self.strtime_end_projected = time.strftime("%H:%M:%S", time.localtime(self.time_end_projected))
            time_hour, time_min, time_sec = self.timestamp(time_taken)
            prog = int(round((self.inc*self.step_size*100)/self.max))
            time_str = "{0:3d}% : {1:3.0f} s ({2:3.0f}:{3:2.0f}:{4:3.1f}) End:{5}".format(prog,time_taken,time_hour, time_min, time_sec,self.strtime_end_projected)
            plogger.info("Monitor: " + time_str)

    # called to get time since last called
    def elapsed_time (self,stage='') :
        stage = stage.ljust(12)
        time_hour, time_min, time_sec = self.timestamp(time.time() - self.time_start)
        _, takenl_m, takenl_s = self.timestamp(time.time() - self.time_last,hour=False)
        tstr = "{:3.0f}m {:.3f}s ({:3.0f}:{:2.0f}:{:3.1f})".format(takenl_m, takenl_s, time_hour, time_min, time_sec)
        plogger.debug('Elapsed Time: ' + tstr + ' - ' + stage)
        self.time_last = time.time()
        return tstr

    def timestamp(self,time_s,hour=True):
        time_hour = 0
        if hour :
            time_hour, time_s = divmod(time_s, 3600)
        time_min, time_sec = divmod(time_s, 60)
        return time_hour, time_min, time_sec

    def get_start_time(self):
        return time.strftime("%H:%M:%S", time.localtime(self.time_start))