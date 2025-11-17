""" 
Course: CSE 351
Team  : 
File  : Week 9 team.py
Author:  Luc Comeau
"""

# Include CSE 351 common Python files. 
from cse351 import *
import time
import random
import multiprocessing as mp

# number of cleaning staff and hotel guests
CLEANING_STAFF = 2
HOTEL_GUESTS = 5


# Run program for this number of seconds
TIME = 60

STARTING_PARTY_MESSAGE =  'Turning on the lights for the party vvvvvvvvvvvvvv'
STOPPING_PARTY_MESSAGE  = 'Turning off the lights  ^^^^^^^^^^^^^^^^^^^^^^^^^^'

STARTING_CLEANING_MESSAGE =  'Starting to clean the room >>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
STOPPING_CLEANING_MESSAGE  = 'Finish cleaning the room <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'

def cleaner_waiting():
    time.sleep(random.uniform(0, 2))

def cleaner_cleaning(id):
    print(f'Cleaner: {id}')
    time.sleep(random.uniform(0, 2))

def guest_waiting():
    time.sleep(random.uniform(0, 2))

def guest_partying(id, count):
    print(f'Guest: {id}, count = {count}')
    time.sleep(random.uniform(0, 1))

def cleaner(id, start_time, guest_count, room_lock, cleaned_count):
    """
    do the following for TIME seconds
        cleaner will wait to try to clean the room (cleaner_waiting())
        get access to the room
        display message STARTING_CLEANING_MESSAGE
        Take some time cleaning (cleaner_cleaning())
        display message STOPPING_CLEANING_MESSAGE
    """
    while time.time() - start_time < TIME:
        cleaner_waiting()
        with room_lock:
            if guest_count.value == 0:
               print(STARTING_CLEANING_MESSAGE)
               cleaner_cleaning(id)
               cleaned_count.value += 1
               print(STOPPING_CLEANING_MESSAGE)

def guest(id, start_time, guest_count, room_lock, party_count):
    """
    do the following for TIME seconds
        guest will wait to try to get access to the room (guest_waiting())
        get access to the room
        display message STARTING_PARTY_MESSAGE if this guest is the first one in the room
        Take some time partying (call guest_partying())
        display message STOPPING_PARTY_MESSAGE if the guest is the last one leaving in the room
    """
    while time.time() - start_time < TIME:
        guest_waiting()
        with room_lock:
            guest_count.value += 1
            if guest_count.value == 1:
                print(STARTING_PARTY_MESSAGE)
                party_count.value += 1
        guest_partying(id, guest_count.value)
        with room_lock:
            guest_count.value -= 1
            if guest_count.value == 0:
                print(STOPPING_PARTY_MESSAGE)
    

def main():
    # Start time of the running of the program.
    start_time = time.time()

    # TODO - add any variables, data structures, processes you need
    room_lock = mp.Lock()
    doorway = mp.Lock()
    guest_count = mp.Value('i', 0)      # shared integer for how many guests are inside
    cleaned_count = mp.Value('i', 0)
    party_count = mp.Value('i', 0)
    # TODO - add any arguments to cleaner() and guest() that you need
    guests = [mp.Process(target= guest, args = (i, start_time, guest_count, room_lock, party_count)) for i in range(HOTEL_GUESTS)]

    cleaners = [mp.Process(target= cleaner, args = (i, start_time, guest_count, room_lock, cleaned_count)) for i in range(CLEANING_STAFF)]
    # Results
    for p in guests + cleaners:
        p.start()
    for p in guests + cleaners:
        p.join()
    print(f'Room was cleaned {cleaned_count.value} times, there were {party_count.value} parties')


if __name__ == '__main__':
    main()
