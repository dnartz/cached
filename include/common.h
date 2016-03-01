#ifndef _COMMON_H_H
#define _COMMON_H_H

#include <cstdio>

#include <netdb.h>
#include <sys/fcntl.h>

namespace cached {

int new_socket(struct addrinfo *ai) noexcept {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        std::perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL)) < 0
        || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        perror("fcntl()");
        return -1;
    }

    return sfd;
}

}

#endif //_COMMON_H_H
