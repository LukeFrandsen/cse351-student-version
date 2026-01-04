using System.Diagnostics;

namespace assignment11;

public class Assignment11
{
    private const long START_NUMBER = 10_000_000_000;
    private const int RANGE_COUNT = 1_000_000;

    private static bool IsPrime(long n)
    {
        if (n <= 3) return n > 1;
        if (n % 2 == 0 || n % 3 == 0) return false;

        for (long i = 5; i * i <= n; i = i + 6)
        {
            if (n % i == 0 || n % (i + 2) == 0)
                return false;
        }
        return true;
    }

    public static void Main(string[] args)
    {
        Console.WriteLine("Starting prime number search...");

        Queue<long> workQueue = new Queue<long>();
        object queueLock = new object();
        object printLock = new object();
        object primeLock = new object();
        bool doneAdding = false;
        int primeCount = 0;
        int workerCount = 10;
        Thread[] workers = new Thread[workerCount];
        var stopwatch = Stopwatch.StartNew();

        for (long i = START_NUMBER; i < START_NUMBER + RANGE_COUNT; i++)
        {
            lock (queueLock)
            {
                workQueue.Enqueue(i);
            }
        }
        doneAdding = true;

        void worker()
        {
            while (true)
            {
                long number = 0;
                lock (queueLock)
                {
                    if (workQueue.Count > 0)
                    {
                        number = workQueue.Dequeue();
                    }
                    else if (doneAdding)
                    {
                        return;
                    }
                }

                if (number != 0 && IsPrime(number))
                {
                    lock (primeLock)
                    {
                        primeCount++;
                    }
                    lock (printLock)
                    {
                        Console.WriteLine($"Prime found: {number}");
                    }
                }
            }
        }

        for (int i = 0; i < workerCount; i++)
        {
            workers[i] = new Thread(worker);
            workers[i].Start();
        }
        foreach (var workerThread in workers)
        {
            workerThread.Join();
        }

        stopwatch.Stop();

        Console.WriteLine();

        // Should find 43427 primes for range_count = 1000000
        Console.WriteLine($"Primes found      = {primeCount}");
        Console.WriteLine($"Worker threads    = {workerCount}");
        Console.WriteLine($"Total time        = {stopwatch.Elapsed}");
    }
}