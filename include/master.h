#ifndef _MASTER_H
#define _MASTER_H

#include <thread>
#include <vector>
#include <list>

#include <sys/socket.h>
#include <sys/types.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>

#include <ev.h>
#include <setting.h>
#include <connection.h>
#include <worker.h>

namespace cached {

class master {
    class listener {
        int sfd;
        ev_io evio;

        socklen_t addr_len;
        struct sockaddr_storage addr;

    public:
        listener(int fd, struct sockaddr *address, socklen_t addr_len);

        void bind_ev_loop(struct ev_loop *loop);
    };

    std::vector<listener> listeners;

    unsigned nworker;
    unsigned last_worker = 0;
    worker *workers;

    struct ev_loop *evloop = EV_DEFAULT;

public:
    master();

    int init_listener() noexcept;

    void start_listen() noexcept;

    void dispatch_new_conn(int fd) noexcept;

    static master& get_instance() {
        static master instance;
        return instance;
    }

    ~master();
};

}

#endif // _MASTER_H