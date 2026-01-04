"""
Course: CSE 351, week 10
File: functions.py
Author: <your name>

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
data = get_data_from_server('{TOP_API_URL}/family/{family_id}')

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
data = get_data_from_server('{TOP_API_URL}/person/{person_id}')

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}


--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

<Add your comments here>
So to speed things up in part 1, I emiminated duplicate work by keeping track of seen families and people using thread-safe sets. 
This prevents redundant API calls for already fetched data. I grabbed each person's data in its own thread, 
allowing multiple requests to be processed concurrently by the server. 
Then I joined the threads at the root level only, so all child threads could run in parallel without waiting for each other.


Describe how to speed up part 2

<Add your comments here>
I use the same memoization technique to avoid duplicate fetches of families and people.
I use a queue to manage the breadth-first traversal instead of recursion.
I spawn a worker thread for each family pulled from the queue, allowing many families to be processed concurrently.


Extra (Optional) 10% Bonus to speed up part 3

<Add your comments here>

"""
from common import *
import queue
import threading


def is_invalid(x):
    return x in (None, "", "0")
# -----------------------------------------------------------------------------
ALL_DFS_THREADS = []

def depth_fs_pedigree(family_id, tree, is_root=True):
    # KEEP this function even if you don't implement it
    # TODO - implement Depth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    
    if not hasattr(depth_fs_pedigree, "seen_fam"):
        depth_fs_pedigree.seen_fam = set()
        depth_fs_pedigree.seen_people = set()
        depth_fs_pedigree.lock = threading.Lock()

    if is_invalid(family_id):
        return

    with depth_fs_pedigree.lock:
        if family_id in depth_fs_pedigree.seen_fam:
            return
        depth_fs_pedigree.seen_fam.add(family_id)

    fam_data = get_data_from_server(f"{TOP_API_URL}/family/{family_id}")
    if fam_data is None:
        return
    family = Family(fam_data)
    tree.add_family(family)
    person_ids = [family.get_husband(), family.get_wife(), *family.get_children()]
    person_threads = []

    for pid in person_ids:
        if is_invalid(pid):
            continue
        to_fetch = False
        with depth_fs_pedigree.lock:
            if pid not in depth_fs_pedigree.seen_people:
                depth_fs_pedigree.seen_people.add(pid)
                to_fetch = True
        if not to_fetch:
            continue
        def _person_worker(person_id):
            pdata = get_data_from_server(f"{TOP_API_URL}/person/{person_id}")
            if pdata:
                tree.add_person(Person(pdata))
        t = threading.Thread(target=_person_worker, args=(pid,))
        t.start()
        person_threads.append(t)
        ALL_DFS_THREADS.append(t)

    for t in person_threads:
        t.join()

    def _spawn_parent(parent_family_id):
        if is_invalid(parent_family_id):
            return
        with depth_fs_pedigree.lock:
            if parent_family_id in depth_fs_pedigree.seen_fam:
                return
        t = threading.Thread(target=depth_fs_pedigree, args=(parent_family_id, tree, False))
        t.start()
        ALL_DFS_THREADS.append(t)

    husband = None
    wife = None
    hid = family.get_husband()
    wid = family.get_wife()
    if not is_invalid(hid):
        husband = tree.get_person(hid)
    if not is_invalid(wid):
        wife = tree.get_person(wid)

    if husband:
        ph = husband.get_parentid()
        _spawn_parent(ph)
    if wife:
        pw = wife.get_parentid()
        _spawn_parent(pw)

    if is_root:
        for t in ALL_DFS_THREADS:
            t.join()
            
#-----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    # TODO - Printing out people and families that are retrieved from the server will help debugging

    if is_invalid(family_id):
        return

    seen_fam = set()
    seen_people = set()
    lock = threading.Lock()

    q = queue.Queue()
    seen_fam.add(family_id)
    q.put(family_id)

    def worker(fid):
        fam_data = get_data_from_server(f"{TOP_API_URL}/family/{fid}")
        if fam_data is None:
            return

        family = Family(fam_data)
        tree.add_family(family)

        person_threads = []
        for pid in [family.get_husband(), family.get_wife(), *family.get_children()]:
            if is_invalid(pid):
                continue
            to_fetch = False
            with lock:
                if pid not in seen_people:
                    seen_people.add(pid)
                    to_fetch = True
            if not to_fetch:
                continue

            def fetch_person(p):
                d = get_data_from_server(f"{TOP_API_URL}/person/{p}")
                if d:
                    tree.add_person(Person(d))

            pt = threading.Thread(target=fetch_person, args=(pid,))
            pt.start()
            person_threads.append(pt)

        for pt in person_threads:
            pt.join()

        h = tree.get_person(family.get_husband())
        if h:
            p = h.get_parentid()
            if not is_invalid(p):
                with lock:
                    if p not in seen_fam:
                        seen_fam.add(p)
                        q.put(p)

        w = tree.get_person(family.get_wife())
        if w:
            p = w.get_parentid()
            if not is_invalid(p):
                with lock:
                    if p not in seen_fam:
                        seen_fam.add(p)
                        q.put(p)

    worker_threads = []

    while True:
        try:
            fid = q.get(timeout=0.01)
        except queue.Empty:
            if not any(t.is_alive() for t in worker_threads):
                break
            continue

        t = threading.Thread(target=worker, args=(fid,))
        t.start()
        worker_threads.append(t)

    for t in worker_threads:
        t.join()


# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
    # KEEP this function even if you don't implement it
    # TODO - implement breadth first retrieval
    #      - Limit number of concurrent connections to the FS server to 5
    # TODO - Printing out people and families that are retrieved from the server will help debugging
    pass