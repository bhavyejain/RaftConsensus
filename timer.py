from threading import Thread, Event

def ElectionTimer(*args, **kwargs):
    """ Global function for Timer """
    return _TimerReset(*args, **kwargs)


class _TimerReset(Thread):
    """Call a function after a specified number of seconds:

    t = TimerReset(30.0, f, args=[], kwargs={})
    t.start()
    t.cancel() # stop the timer's action if it's still waiting
    """

    def __init__(self, interval, function, args=[], kwargs={}):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.finished = Event()
        self.resetted = True
        self.running = True

    def cancel(self):
        """Stop the timer if it hasn't finished yet"""
        self.finished.set()

    def run(self):
        # print "Time: %s - timer running..." % time.asctime()

        while True:
            if self.running:
                while self.resetted:
                    # print "Time: %s - timer waiting for timeout in %.2f..." % (time.asctime(), self.interval)
                    self.resetted = False
                    self.finished.wait(self.interval)
                
                self.running = False

                if not self.finished.isSet():
                    self.function(*self.args, **self.kwargs)
                
                # self.finished.set()
                # print "Time: %s - timer finished!" % time.asctime()
    
    def restart(self):
        self.reset()
        self.running = True

    def reset(self, interval=None):
        """ Reset the timer """

        if interval:
            # print "Time: %s - timer resetting to %.2f..." % (time.asctime(), interval)
            self.interval = interval
        # else:
            # print "Time: %s - timer resetting..." % time.asctime()

        self.resetted = True
        self.finished.set()
        self.finished.clear()
