#ifndef _WORKER_H
#define _WORKER_H

#include <list>
#include <mutex>

#include <ev.h>

#include <connection.h>

namespace cached {

class worker {
    int read_pipe;
    int write_pipe;
    ev_io read_pipe_evio;

    std::mutex wait_queue_mtx;
    std::list<int> wait_queue;

    std::list<connection> conns;

public:
    struct ev_loop *evloop = EV_DEFAULT;

    worker();

    worker(const worker& w) = delete;
    worker& operator=(const worker& w) = delete;

    void dispatch_new_conn(int fd) noexcept;

    static void recv_new_conn_sig(EV_P_ ev_io *w, int revents) noexcept;

    void inline remove_conn(connection& conn) noexcept {
        this->conns.erase(++conn.get_prev_iterator());
    }
};

}

#endif // _WORKER_H
