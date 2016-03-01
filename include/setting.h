#ifndef _SETTING_H
#define _SETTING_H

#include <sys/socket.h>

namespace cached {

class setting {
public:
    int backlog = 1024;
    int listen_port = 23333;
    int socket_domain = AF_INET;
    int socket_type = SOCK_STREAM;

    unsigned int max_exptime = 60 * 60 * 24 * 30;

    size_t max_key_len = 250;
    size_t max_item_size = 1024 * 1024;

    size_t cl_read_buffer_size = 2048;

    const char *interface = nullptr;
    const char *listen_addr = "127.0.0.1";
    const char *unix_socket_path = nullptr;

    static setting&get_instance() {
        static setting instance;
        return instance;
    }
};

}

#endif //_SETTING_H
