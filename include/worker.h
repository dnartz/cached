#ifndef _WORKER_H
#define _WORKER_H

#include <list>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <ev.h>

#include <connection.h>

namespace cached {

class worker {
    int read_pipe;
    int write_pipe;
    ev_io read_pipe_evio;

    std::thread *work_thread;

    std::mutex wait_queue_mtx;
    std::list<int> wait_queue;

    std::unordered_map<int, connection> conns;

public:
    struct ev_loop *evloop;

    worker();

    worker(const worker& w) = delete;
    worker& operator=(const worker& w) = delete;

    void dispatch_new_conn(int fd) noexcept;

    static void recv_master_sig(EV_P_ ev_io *w, int revents) noexcept;

    void run_thread();

    static void run(worker& w) noexcept;

    void inline remove_conn(connection& conn) noexcept {
        this->conns.erase(conn.sfd);
    }
};

}

#endif // _WORKER_H
