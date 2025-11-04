import time
import threading
from queue import Queue
from common import *
from cse351 import *

# Tune these (10 is a safe default)
THREADS = 100
WORKERS = 100
RECORDS_TO_RETRIEVE = 5000  # don't change

# ---------------------------------------------------------------------------
def retrieve_weather_data(q_commands, q_results, counters, tid):
    """
    Thread function: consume (city,recno) from q_commands,
    fetch from server, put (city,date,temp) into q_results.
    Retries on temporary errors until success.
    """
    while True:
        task = q_commands.get()
        if task is None:
            q_commands.task_done()
            # print for debugging
            print(f"[retriever-{tid}] received STOP")
            break

        city, recno = task
        # keep retrying until we get a response (server transient fail protection)
        while True:
            try:
                data = get_data_from_server(f'{TOP_API_URL}/record/{city}/{recno}')
                break
            except Exception as e:
                # print and backoff a tiny bit
                print(f"[retriever-{tid}] ERROR fetching {city}-{recno}: {e} -- retrying")
                time.sleep(0.2)

        # push result to workers
        q_results.put((city, data.get('date'), data.get('temp')))

        # bookkeeping
        with counters['lock']:
            counters['retrieved'] += 1
            # periodic progress print
            if counters['retrieved'] % 5000 == 0 or counters['retrieved'] <= 5:
                print(f"[progress] retrieved {counters['retrieved']} / {counters['total_tasks']}")
        q_commands.task_done()

    # thread exits
    return

# ---------------------------------------------------------------------------
class Worker(threading.Thread):
    def __init__(self, q_results, noaa, counters, wid):
        super().__init__()
        self.q_results = q_results
        self.noaa = noaa
        self.counters = counters
        self.wid = wid

    def run(self):
        while True:
            item = self.q_results.get()
            if item is None:
                self.q_results.task_done()
                print(f"[worker-{self.wid}] received STOP")
                break

            city, date, temp = item
            # store temp (NOAA handles locking)
            self.noaa.add_record(city, date, temp)

            with self.counters['lock']:
                self.counters['processed'] += 1
                if self.counters['processed'] % 5000 == 0 or self.counters['processed'] <= 5:
                    print(f"[progress] processed {self.counters['processed']} / {self.counters['total_tasks']}")

            self.q_results.task_done()

# ---------------------------------------------------------------------------
class NOAA:
    def __init__(self):
        # store only temps (get_temp_details only needs temps)
        self.data = {city: [] for city in CITIES}
        self.lock = threading.Lock()

    def add_record(self, city, date, temp):
        # guard append with lock
        with self.lock:
            # parse/validate temp if needed
            try:
                self.data[city].append(float(temp))
            except Exception:
                # if something odd happens, still append 0.0 to keep counts consistent
                print(f"[NOAA] bad temp for {city} on {date}: {temp} -- storing 0.0")
                self.data[city].append(0.0)

    def get_temp_details(self, city):
        with self.lock:
            temps = self.data.get(city, [])
            if not temps:
                return 0.0
            return sum(temps) / len(temps)

# ---------------------------------------------------------------------------
def monitor_progress(counters):
    """Print live progress every 2 seconds until done."""
    while True:
        with counters['lock']:
            retrieved = counters['retrieved']
            processed = counters['processed']
            total = counters['total_tasks']
            done = counters.get('done', False)
        elapsed = time.time() - counters['start_time']
        print(f"[monitor] elapsed {elapsed:.1f}s — retrieved {retrieved}/{total} processed {processed}/{total}")
        if done:
            break
        time.sleep(2)
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

    # Start server handshake
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Retrieve city metadata
    print('Retrieving city details')
    city_details = {}
    print(f'{"City":>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
    print('===================================')

    # Prepare queues (spec says limit size to 10)
    q_commands = Queue(maxsize=10)
    q_results = Queue(maxsize=10)

    # Counters and monitor
    total_tasks = len(CITIES) * RECORDS_TO_RETRIEVE
    counters = {
        'retrieved': 0,
        'processed': 0,
        'total_tasks': total_tasks,
        'lock': threading.Lock(),
        'start_time': time.time(),
        'done': False
    }

    # Start monitor thread
    monitor = threading.Thread(target=monitor_progress, args=(counters,), daemon=True)
    monitor.start()

    # Start retriever threads
    retrievers = []
    for i in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(q_commands, q_results, counters, i), daemon=True)
        t.start()
        retrievers.append(t)

    # Start worker threads
    workers = []
    for i in range(WORKERS):
        w = Worker(q_results, noaa, counters, i)
        w.start()
        workers.append(w)

    # Enqueue all tasks
    print("[main] enqueueing tasks...")
    for city in CITIES:
        for recno in range(RECORDS_TO_RETRIEVE):
            q_commands.put((city, recno))

    # Send stop tokens for retrievers (one per retriever)
    for _ in retrievers:
        q_commands.put(None)

    # Wait until all retrieval tasks processed (and retrievers consumed sentinels)
    q_commands.join()
    print("[main] all commands consumed by retrievers")

    # Now there will be some items in q_results; signal workers to stop AFTER they process everything.
    for _ in workers:
        q_results.put(None)

    # Wait until all results processed
    q_results.join()
    print("[main] all results processed by workers")

    # join threads (clean exit)
    for t in retrievers:
        t.join(timeout=1)
    for w in workers:
        w.join(timeout=1)

    # end monitor
    with counters['lock']:
        counters['done'] = True

    # End server
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    # Verify
    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')

if __name__ == '__main__':
    main()
