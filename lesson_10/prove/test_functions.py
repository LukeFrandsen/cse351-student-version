"""
Course: CSE 351, week 10
File: functions.py
Author: Luke Frandsen

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04

Describe how to speed up part 1

Depth-first (part 1) can be sped up by:
1) Eliminating duplicate work with memoization: keep a thread-safe set of seen family IDs and seen person IDs to avoid re-requesting the same resource.
2) Parallelizing independent network calls: fetch each person's data in its own thread so the server can answer multiple requests in parallel.
3) Minimizing lock hold times: only guard small checks/insertions into memo sets with a lock; do not hold locks during network requests.
4) Joining threads at the root only: spawn threads during recursion, but only join all spawned threads at the top-level call so child work runs concurrently.
These steps together reduce duplicate API calls and exploit server concurrency, significantly reducing runtime.

Describe how to speed up part 2

Breadth-first (part 2) can be sped up by:
1) Same memoization technique to prevent duplicate family/person fetches.
2) Avoid recursion; use an explicit queue for BFS.
3) Parallelize processing of each queued family: launch a worker thread for each family pulled from the queue so many families are processed concurrently.
4) Use a thread-safe enqueue mechanism and keep lock hold minimal.

Extra (Optional) 10% Bonus to speed up part 3



"""
from common import *
import queue
import threading


def _is_invalid_id(x):
    return x in (None, "", "0")


ALL_DFS_THREADS = []  

def depth_fs_pedigree(family_id, tree, is_root=True):
    # initialize static seen sets and lock
    if not hasattr(depth_fs_pedigree, "seen_fam"):
        depth_fs_pedigree.seen_fam = set()
        depth_fs_pedigree.seen_people = set()
        depth_fs_pedigree.lock = threading.Lock()

    # invalid or nothing to do
    if _is_invalid_id(family_id):
        return

    # check and mark family as seen (thread-safe)
    with depth_fs_pedigree.lock:
        if family_id in depth_fs_pedigree.seen_fam:
            return
        depth_fs_pedigree.seen_fam.add(family_id)

    # fetch family data (network call OUTSIDE locks)
    fam_data = get_data_from_server(f"{TOP_API_URL}/family/{family_id}")
    if fam_data is None:
        return

    family = Family(fam_data)
    tree.add_family(family)

    # -------------------------
    # Fetch people in parallel for this family
    # -------------------------
    person_ids = [family.get_husband(), family.get_wife(), *family.get_children()]
    person_threads = []
    # we'll collect fetched person ids to later inspect parent_id
    # but we don't need their JSON here because tree stores Person after add_person
    for pid in person_ids:
        if _is_invalid_id(pid):
            continue

        # check and claim pid for fetching
        to_fetch = False
        with depth_fs_pedigree.lock:
            if pid not in depth_fs_pedigree.seen_people:
                depth_fs_pedigree.seen_people.add(pid)
                to_fetch = True

        if not to_fetch:
            continue

        # spawn thread to fetch person and add to tree
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

    parent_threads = []

    def _spawn_parent(parent_family_id):
        if _is_invalid_id(parent_family_id):
            return
        # avoid duplicate spawn races by checking only (do not pre-add)
        with depth_fs_pedigree.lock:
            if parent_family_id in depth_fs_pedigree.seen_fam:
                return
        # spawn recursion thread — the child will mark the family as seen at entry
        t = threading.Thread(target=depth_fs_pedigree, args=(parent_family_id, tree, False))
        t.start()
        parent_threads.append(t)
        ALL_DFS_THREADS.append(t)

    # get husband/wife Person objects (may or may not exist)
    husband = None
    wife = None
    hid = family.get_husband()
    wid = family.get_wife()
    if not _is_invalid_id(hid):
        husband = tree.get_person(hid)
    if not _is_invalid_id(wid):
        wife = tree.get_person(wid)

    if husband:
        ph = husband.get_parentid()
        _spawn_parent(ph)
    if wife:
        pw = wife.get_parentid()
        _spawn_parent(pw)

    # do not join parent threads here; they will run concurrently
    # Root caller (is_root True) must wait for all DFS threads to finish
    if is_root:
        # Wait for any threads spawned during the entire DFS (ALL_DFS_THREADS)
        for t in ALL_DFS_THREADS:
            t.join()

# -----------------------------------------------------------------------------
# Breadth-First Pedigree Retrieval (threaded, non-recursive)
# -----------------------------------------------------------------------------
def breadth_fs_pedigree(family_id, tree):
    # Initialize static structures
    breadth_fs_pedigree.seen_fam = set()
    breadth_fs_pedigree.seen_people = set()
    breadth_fs_pedigree.threads = []
    breadth_fs_pedigree.lock = threading.Lock()

    q = queue.Queue()
    # enqueue start if valid
    if _is_invalid_id(family_id):
        return
    with breadth_fs_pedigree.lock:
        breadth_fs_pedigree.seen_fam.add(family_id)
    q.put(family_id)

    # worker processes one family (fetch family, fetch people, enqueue parents)
    def worker(fam_id):
        fam_data = get_data_from_server(f"{TOP_API_URL}/family/{fam_id}")
        if fam_data is None:
            return
        family = Family(fam_data)
        tree.add_family(family)

        # fetch people sequentially or in small threads
        # here we spawn per-person threads to better utilize server concurrency
        local_person_threads = []
        person_ids = [family.get_husband(), family.get_wife(), *family.get_children()]

        for pid in person_ids:
            if _is_invalid_id(pid):
                continue
            to_fetch = False
            with breadth_fs_pedigree.lock:
                if pid not in breadth_fs_pedigree.seen_people:
                    breadth_fs_pedigree.seen_people.add(pid)
                    to_fetch = True
            if not to_fetch:
                continue

            def _p_worker(person_id):
                pdata = get_data_from_server(f"{TOP_API_URL}/person/{person_id}")
                if pdata:
                    tree.add_person(Person(pdata))

            pt = threading.Thread(target=_p_worker, args=(pid,))
            pt.start()
            local_person_threads.append(pt)
            breadth_fs_pedigree.threads.append(pt)

        # wait for people of this family to be added so we can read parent ids
        for pt in local_person_threads:
            pt.join()

        # enqueue parent families (thread-safe)
        h = tree.get_person(family.get_husband()) if not _is_invalid_id(family.get_husband()) else None
        if h:
            p = h.get_parentid()
            if not _is_invalid_id(p):
                with breadth_fs_pedigree.lock:
                    if p not in breadth_fs_pedigree.seen_fam:
                        breadth_fs_pedigree.seen_fam.add(p)
                        q.put(p)

        w = tree.get_person(family.get_wife()) if not _is_invalid_id(family.get_wife()) else None
        if w:
            p = w.get_parentid()
            if not _is_invalid_id(p):
                with breadth_fs_pedigree.lock:
                    if p not in breadth_fs_pedigree.seen_fam:
                        breadth_fs_pedigree.seen_fam.add(p)
                        q.put(p)

    # main loop: dequeue families, start worker threads
    worker_threads = []

    while True:
        try:
            fam = q.get(timeout=0.01)  # wait briefly for worker threads to enqueue parents
        except queue.Empty:
            # If queue empty AND no worker thread is alive → BFS is done
            if not any(t.is_alive() for t in worker_threads):
                break
            else:
                continue  # workers still running, let them enqueue more families

        # Start worker for this family
        t = threading.Thread(target=worker, args=(fam,))
        t.start()
        worker_threads.append(t)
        breadth_fs_pedigree.threads.append(t)

    # join all worker & person threads started
    for t in worker_threads:
        t.join()
    for t in list(breadth_fs_pedigree.threads):
        # threads might already be joined, join is idempotent
        t.join()

# -----------------------------------------------------------------------------
# Breadth-First Pedigree Retrieval with maximum 5 concurrent threads
# -----------------------------------------------------------------------------
def breadth_fs_pedigree_limit5(family_id, tree):
   pass