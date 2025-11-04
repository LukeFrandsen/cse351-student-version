"""
Course    : CSE 351
Assignment: 04
Student   : <your name here>

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
from common import *
import threading
import queue

from cse351 import *

THREADS = 1000
WORKERS = 1000
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(task_queue, result_queue):
    while True:
        task = task_queue.get()
        if task is None or task == "DONE":
            # Mark the DONE task as processed and exit
            task_queue.task_done()
            break

        city, recno = task
        record = get_data_from_server(f'{TOP_API_URL}/record/{city}/{recno}')

        city_name = city
        date = record['date']
        temp = record['temp']

        result_queue.put((city_name, record.get("date"), record.get("temp")))
        task_queue.task_done()


# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class
class Worker(threading.Thread):
    def __init__(self, result_queue, noaa):
        super().__init__()
        self.result_queue = result_queue
        self.noaa = noaa

    
    def run(self):
        while True:
            data = self.result_queue.get()
            if data is None or data == "DONE":
                self.result_queue.task_done()
                break
            city, date, temp = data
            self.noaa.store(city, date, temp)
            self.result_queue.task_done()


# ---------------------------------------------------------------------------
# TODO - Complete this class
class NOAA:
    def __init__(self):
        self.results = {city: [] for city in CITIES}
        self.lock = threading.Lock()

    def store(self, city, date, temp):
        with self.lock:
            self.results[city].append((float(temp)))

    def get_temp_details(self, city):
        with self.lock:
            temps = self.results.get(city, [])
            if not temps:
                return 0.0
            return sum(temps) / len(temps)


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need
    task_queue = queue.Queue(maxsize=10)
    result_queue = queue.Queue(maxsize=10)
    
    retrievers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(task_queue, result_queue))
        t.start()
        retrievers.append(t)
    
   
    workers = []
    for i in range(WORKERS):
        w = Worker(result_queue, noaa)
        w.start()
        workers.append(w)

    
    for city in CITIES:
        for recno in range(records):
            task_queue.put((city, recno))

    for _ in retrievers:
        task_queue.put("DONE")
    task_queue.join()
   
    for _ in workers:
        result_queue.put("DONE")
    result_queue.join()

    for t in retrievers:
        t.join(timeout=1)
    for w in workers:
        w.join(timeout=1)

    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)
    
    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()

