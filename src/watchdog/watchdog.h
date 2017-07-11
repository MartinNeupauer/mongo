/**
 * Copyright (C) 2017 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <boost/filesystem/path.hpp>
#include <string>
#include <vector>

#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/duration.h"

namespace mongo {

class OperationContext;

/**
 * WatchdogDeathCallback is used by the watchdog component to terminate the process. It is expected
 * to bypass MongoDB's normal shutdown process. It should not make any syscalls other then to
 * exit/terminate the process.
 *
 * It is pluggable for testing purposes.
 */
using WatchdogDeathCallback = stdx::function<void(void)>;

/**
 * The OS specific implementation of WatchdogDeathCallback that kills the process.
 */
void watchdogTerminate();

/**
 * WatchdogCheck represents a health check that the watchdog will run periodically to ensure the
 * machine, and process are healthy.
 *
 * It is pluggable for testing purposes.
 */
class WatchdogCheck {
public:
    /**
     * Runs a health check against the local machine.
     *
     * Note: It should throw exceptions on unexpected errors. Exceptions will result in a call to
     * WatchdogDeathCallback.
     */
    virtual void run(OperationContext* opCtx) = 0;

    /**
     * Returns a description for the watchdog check to log to the log file.
     */
    virtual std::string getDescriptionForLogging() = 0;
};

/**
 * Do a health check for a given directory. This health check is done by reading, and writing to a
 * file with direct I/O.
 */
class DirectoryCheck : public WatchdogCheck {
public:
    static constexpr StringData kProbeFileName = "watchdog_probe.txt"_sd;

public:
    DirectoryCheck(const boost::filesystem::path& directory) : _directory(directory) {}

    void run(OperationContext* opCtx) final;

    std::string getDescriptionForLogging() final;

private:
    boost::filesystem::path _directory;
};

/**
 * Runs a callback on a periodic basis. The specified time period is the time delay between
 * invocations.
 *
 * Example:
 * - callback
 * - sleep(period)
 * - callback
 */
class WatchdogPeriodicThread {
public:
    WatchdogPeriodicThread(Milliseconds period, StringData threadName);

    /**
     * Starts the periodic thread.
     */
    void start();

    /**
     * Updates the period the thread runs its task.
     *
     * Period changes take affect immediately.
     */
    void setPeriod(Milliseconds period);

    /**
     * Shutdown the periodic thread. After it is shutdown, it cannot be started.
     */
    void shutdown();

protected:
    /**
     * Do one iteration of work.
     */
    virtual void run(OperationContext* opCtx) = 0;

    /**
     * Provides an opportunity for derived classes to initialize state.
     *
     * This method is called at two different times:
     * 1. First time a thread is started.
     * 2. When a thread goes from disabled to enabled. Specifically, a user calls setPeriod(-1)
     *    followed by setPeriod(> 0).
     *
     */
    virtual void resetState() = 0;

private:
    /**
     * Main thread loop
     */
    void doLoop();

private:
    /**
     * Private enum to track state.
     *
     *   +----------------------------------------------------------------+
     *   |                                                                v
     * +-------------+     +----------+     +--------------------+     +-------+
     * | kNotStarted | --> | kStarted | --> | kShutdownRequested | --> | kDone |
     * +-------------+     +----------+     +--------------------+     +-------+
     */
    enum class State {
        /**
         * Initial state. Either start() or shutdown() can be called next.
         */
        kNotStarted,

        /**
         * start() has been called. shutdown() should be called next.
         */
        kStarted,

        /**
         * shutdown() has been called, and the thread is in progress of shutting down.
         */
        kShutdownRequested,

        /**
         * PeriodicThread has been shutdown.
         */
        kDone,
    };

    // State of PeriodicThread
    State _state{State::kNotStarted};

    // Thread period
    Milliseconds _period;

    // if true, then call run() otherwise just let the thread idle,
    bool _enabled;

    // Name of thread for logging purposes
    std::string _threadName;

    // The thread
    stdx::thread _thread;

    // Lock to protect _state and control _thread
    stdx::mutex _mutex;
    stdx::condition_variable _condvar;
};

/**
 * Periodic background thread to run watchdog checks.
 */
class WatchdogCheckThread : public WatchdogPeriodicThread {
public:
    WatchdogCheckThread(std::vector<std::unique_ptr<WatchdogCheck>> checks, Milliseconds period);

    /**
     * Returns the current generation number of the checks.
     *
     * Incremented after each check is run.
     */
    std::int64_t getGeneration();

private:
    void run(OperationContext* opCtx) final;
    void resetState() final;

private:
    // Vector of checks to run
    std::vector<std::unique_ptr<WatchdogCheck>> _checks;

    // A counter that is incremented for each watchdog check completed, and monitored to ensure it
    // does not remain at the same value for too long.
    AtomicInt64 _checkGeneration{0};
};

/**
 * Periodic background thread to ensure watchdog checks run periodically.
 */
class WatchdogMonitorThread : public WatchdogPeriodicThread {
public:
    WatchdogMonitorThread(WatchdogCheckThread* checkThread,
                          WatchdogDeathCallback callback,
                          Milliseconds period);

    /**
     * Returns the current generation number of the monitor.
     *
     * Incremented after each round of monitoring is run.
     */
    std::int64_t getGeneration();

private:
    void run(OperationContext* opCtx) final;
    void resetState() final;

private:
    // Callback function to call when watchdog gets stuck
    const WatchdogDeathCallback _callback;

    // Watchdog check thread to query
    WatchdogCheckThread* _checkThread;

    // A counter that is incremented for each watchdog monitor run is completed.
    AtomicInt64 _monitorGeneration{0};

    // The last seen _checkGeneration value
    std::int64_t _lastSeenGeneration{-1};
};


/**
 * WatchdogMonitor
 *
 * The Watchdog is a pair of dedicated threads that try to figure out if a process is hung
 * and terminate if it is. The worst case scenario in a distributed system is a process that appears
 * to work but does not actually work.
 *
 * The watchdog is not designed to detect all the different ways the process is hung. It's goal is
 * to detect if the storage system is stuck, and to terminate the process if it is stuck.
 *
 * Threads:
 * WatchdogCheck - runs file system checks
 * WatchdogMonitor - verifies that WatchdogCheck continue to make timely progress. If WatchdogCheck
 *                   fails to make process, WatchdogMonitor calls a callback. The callback is not
 *                   expected to do any I/O and minimize the system calls it makes.
 */
class WatchdogMonitor {
public:
    /**
     * Create the watchdog with specified period.
     *
     * checkPeriod - how often to run the checks
     * monitorPeriod - how often to run the monitor, must be >= checkPeriod
     */
    WatchdogMonitor(std::vector<std::unique_ptr<WatchdogCheck>> checks,
                    Milliseconds checkPeriod,
                    Milliseconds monitorPeriod,
                    WatchdogDeathCallback callback);

    /**
     * Starts the watchdog threads.
     */
    void start();

    /**
     * Updates the watchdog monitor period. The goal is to detect a failure in the time of the
     * period.
     *
     * Does nothing if watchdog is not started. If watchdog was started, it changes the monitor
     * period, but not the check period.
     *
     * Accepts Milliseconds for testing purposes while the setParameter only works with seconds.
     */
    void setPeriod(Milliseconds duration);

    /**
     * Shutdown the watchdog.
     */
    void shutdown();

    /**
     * Returns the current generation number of the checks.
     *
     * Incremented after each round of checks is run.
     */
    std::int64_t getCheckGeneration();

    /**
     * Returns the current generation number of the checks.
     *
     * Incremented after each round of checks is run.
     */
    std::int64_t getMonitorGeneration();

private:
    /**
     * Private enum to track state.
     *
     *   +----------------------------------------------------------------+
     *   |                                                                v
     * +-------------+     +----------+     +--------------------+     +-------+
     * | kNotStarted | --> | kStarted | --> | kShutdownRequested | --> | kDone |
     * +-------------+     +----------+     +--------------------+     +-------+
     */
    enum class State {
        /**
         * Initial state. Either start() or shutdown() can be called next.
         */
        kNotStarted,

        /**
         * start() has been called. shutdown() should be called next.
         */
        kStarted,

        /**
         * shutdown() has been called, and the background threads are in progress of shutting down.
         */
        kShutdownRequested,

        /**
         * Watchdog has been shutdown.
         */
        kDone,
    };

    // Lock to protect _state and control _thread
    stdx::mutex _mutex;

    // State of watchdog
    State _state{State::kNotStarted};

    // Fixed period for running the checks.
    Milliseconds _checkPeriod;

    // WatchdogCheck Thread - runs checks
    WatchdogCheckThread _watchdogCheckThread;

    // WatchdogMonitor Thread - watches _watchdogCheckThread
    WatchdogMonitorThread _watchdogMonitorThread;
};

}  // namespace mongo
