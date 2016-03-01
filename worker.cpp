#include <mutex>
#include <cstdlib>
#include <cstdio>

#include <unistd.h>
#include <sys/uio.h>
#include <sysexits.h>

#include <worker.h>

namespace cached {

worker::worker() {
    int p[2];
    if (pipe(p) == -1) {
        perror("cannot create pipe for worker thread");
        exit(EX_OSERR);
    }

    this->read_pipe = p[0];
    this->write_pipe = p[1];

    ev_io_init(&this->read_pipe_evio, worker::recv_new_conn_sig, this->read_pipe, EV_READ);

    ev_io_start(this->evloop, &this->read_pipe_evio);
}

void worker::recv_new_conn_sig(EV_P_ ev_io *evio, int revents) noexcept {
    static const auto offsetof_ev_io =
            reinterpret_cast<uint64_t>(&((worker *)0)->read_pipe_evio);

    worker *w = reinterpret_cast<worker *>((uint64_t)(evio) - offsetof_ev_io);
    int cfd;

    char buf;
    auto nread = read(evio->fd, &buf, 1);

    if (nread == 1) {
        {
            std::lock_guard<std::mutex> guard(w->wait_queue_mtx);
            if (!w->wait_queue.empty()) {
                cfd = w->wait_queue.front();
                w->wait_queue.pop_front();
            }
        }

        w->conns.emplace_back(cfd, *w, w->conns.end());
    } else if (nread == 0) {
        fprintf(stderr, "unexpected pipe close");
        exit(1);
    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
        perror("error read waiting conn queue");
    }
}

void worker::dispatch_new_conn(int fd) noexcept {
    {
        std::lock_guard<std::mutex> guard(this->wait_queue_mtx);
        this->wait_queue.push_back(fd);
    }

    if (write(this->write_pipe, "c", 1) != 1) {
        perror("cannot write to worker pipe");
    }
}

}