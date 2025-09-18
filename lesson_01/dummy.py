from cse351 import *
import threading
import time

def print_cool_stuff(l, name):
    time.sleep(.5)
    with l:
        print("Cool stuff!- ", name)
    


l = threading.Lock()
t = threading.Thread(target=print_cool_stuff, args=(l,"Thread-1"))
t2 = threading.Thread(target=print_cool_stuff, args=(l,"Thread-2"))
t.start()
t2.start()
l.acquire()
# Critical section
print("Hello, World!")
l.release()

t.join()
t2.join()

print("Done!")