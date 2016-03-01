#include <thread>

#include <master.h>
#include <common.h>

#include <ev.h>
#include <sys/socket.h>

namespace cached {

master::listener::listener(int fd, struct sockaddr *address, socklen_t addr_len):
sfd(fd),
addr_len(addr_len)
{
    std::memcpy(&this->addr, address, addr_len);

    ev_io_init(&this->evio, [](EV_P_ ev_io *w, int revents) -> void {
        static auto master = master::get_instance();

        socklen_t address_len;
        struct sockaddr_storage address;

        int fd = accept(w->fd, (struct sockaddr *)&address, &address_len);
        if (fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            } else {
                perror("accept()");
                return;
            }
        }

        master.dispatch_new_conn(fd);
    }, this->sfd, EV_READ);
}

void master::dispatch_new_conn(int fd) noexcept {
    this->last_worker = ++this->last_worker % this->nworker;
    worker& w = this->workers[this->last_worker];
    w.dispatch_new_conn(fd);
}

void master::listener::bind_ev_loop(struct ev_loop *loop) {
    ev_io_start(loop, &this->evio);
}

int master::init_listener() noexcept {
    auto setting = setting::get_instance();
    static int flags = 1;

    struct addrinfo *ai;

    struct addrinfo hints = {
            .ai_flags = AI_PASSIVE,
            .ai_family = setting.socket_domain,
            .ai_socktype = setting.socket_type
    };

    static const struct linger ling = {0, 0};

    char port_buf[NI_MAXSERV];
    snprintf(port_buf, sizeof(port_buf), "%d", setting.listen_port);

    auto error = getaddrinfo(setting.interface, port_buf, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM) {
            std::fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        } else {
            std::perror("getaddrinfo()");
        }

        return error;
    }

    for (auto p = ai; p; p = p->ai_next) {
        int sfd;

        if ((sfd = new_socket(p)) == -1) {
            continue;
        }

        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR,
                       (void *) &flags, sizeof(flags)) != 0
            || setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE,
                          (void *) &flags, sizeof(flags)) != 0
            || setsockopt(sfd, SOL_SOCKET, SO_LINGER,
                          (void *) &ling, sizeof(ling)) != 0
            || setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY,
                          (void *) &flags, sizeof(flags)) != 0
                ) {
            perror("setsockopt()");
            close(sfd);
            continue;
        }

        if (bind(sfd, p->ai_addr, p->ai_addrlen) == -1) {
            perror("setsockopt()");
            close(sfd);
            continue;
        }

        if (listen(sfd, setting.backlog) == -1) {
            perror("listen");
            close(sfd);
            continue;
        }

        this->listeners.emplace_back(sfd, ai->ai_addr, ai->ai_addrlen);
    }

    freeaddrinfo(ai);
    return static_cast<int>(this->listeners.size());
}

void master::start_listen() noexcept {
    this->init_listener();

    for (auto listener : this->listeners) {
        listener.bind_ev_loop(this->evloop);
    }

    ev_run(this->evloop, 0);
}

master::master() :
workers(new worker[std::thread::hardware_concurrency()]),
nworker(std::thread::hardware_concurrency())
{ }

master::~master()  {
    ev_loop_destroy(this->evloop);
    delete this->workers;
}

}

int main(int argc, char **argv) {
    cached::master::get_instance().start_listen();
}