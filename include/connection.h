#ifndef _CONNECTION_H
#define _CONNECTION_H

#include <vector>
#include <string>
#include <cstdlib>

#include <sys/socket.h>
#include <ev.h>

#include <assoc.h>

namespace cached {

class worker;

class connection {
public:
    enum class conn_state {
        WAIT_CMD,
        READ_CMD_BUF,
        PARSE_CMD,
        CLOSED
    };

    enum class cmd_parse_state {
        SWALLOW_SPACE,
        SWALLOW_NEW_LINE,
        CMD_NAME,
        KEY,
        FLAG,
        EXPTIME,
        ITEM_SIZE,
        ITEM,
        CAS_KEY,
        SUCCESS
    };

    enum class cmd_parse_result {
        ERROR,
        BUF_EMPTY,
        FINISH
    };

    enum class read_cmd_result {
        READ_ERROR,
        MEMORY_ERROR,
        NOTHING,
        SUCCESS,
    };

    enum class cmd_type {
        CAS,
        SET,
        ADD,
        GET,
        GETS,
        APPEND,
        PREPEND,
        REPLACE,
        DELETE
    };

#define FOREACH_COMMAND(x)\
    x("set", connection::cmd_type::SET)\
    x("add", connection::cmd_type::ADD)\
    x("get", connection::cmd_type::GET)\
    x("gets", connection::cmd_type::GETS)\
    x("append", connection::cmd_type::APPEND)\
    x("prepend", connection::cmd_type::PREPEND)\
    x("replace", connection::cmd_type::REPLACE)\
    x("delete", connection::cmd_type::DELETE)

private:
    int sfd;

    worker &worker_base;

    char *rcurr;
    char *rbuf;

    char *ritem_buf;
    size_t ritem_saved;
    size_t ritem_buf_len;

    size_t r_size;
    size_t r_unparsed;

    char *wcurr;
    char *wbuf;
    size_t w_size;
    size_t w_unwrite;
    bool wevent_bound;

    conn_state state;
    cmd_parse_state parse_state_curr;
    cmd_parse_state next_parse_state;

    cmd_type cmd_curr;
    std::vector<std::string> cmd_key;

    std::string numbuf;
    uint32_t cmd_flag;
    uint32_t cmd_exptime;
    uint64_t cmd_cas_key;
    size_t cmd_item_size;

    ev_io read_evio;
    ev_io write_evio;

    std::list<connection>::iterator prev_iterator;

    void execute_command() noexcept;

    void execute_get(bool return_cas = false) noexcept;

    inline void execute_gets() noexcept {
        this->execute_get(true);
    };

    void execute_delete() noexcept;

    void execute_add() noexcept;

    void execute_prepend_or_append(item_ptr &it,
                                   bucket *&bp,
                                   bool append) noexcept;

    void execute_replace(item_ptr &it, bucket *&bp) noexcept;

    void execute_cas(item_ptr &it, bucket *&bp) noexcept;

    void wbuf_append(const char *buf, size_t size) noexcept;

    inline void wbuf_append(const char * buf) noexcept {
        this->wbuf_append(buf, std::strlen(buf));
    }
public:
    connection(int fd, worker &w, std::list<connection>::iterator end);

    ~connection();

    read_cmd_result try_read_command() noexcept;

    cmd_parse_result try_parse_command() noexcept;

    template<typename T>
    cmd_parse_result try_parse_number(T &res) noexcept {
        while (*this->rcurr >= '0' && *this->rcurr <= '9' && this->r_unparsed > 0)
        {
            this->numbuf.append(this->rcurr, 1);
            this->rcurr++;
            this->r_unparsed--;
        }

        if (this->r_unparsed > 0) {
            if (this->numbuf.size() > 0) {
                res = static_cast<T>(std::atoll(this->numbuf.c_str()));
                this->numbuf.clear();

                return cmd_parse_result::FINISH;
            } else {
                return cmd_parse_result::ERROR;
            }
        } else {
            return cmd_parse_result::BUF_EMPTY;
        }
    }

    static void drive_machine(EV_P_ ev_io *w, int revents) noexcept;

    static void write_response(EV_P_ ev_io *w, int revents) noexcept;

    static inline connection &get_connection(ev_io *w) noexcept {
        static const auto offset = reinterpret_cast<uint64_t>(&((connection *) 0)->read_evio);
        return *reinterpret_cast<connection *>((uint64_t) (w) - offset);
    }

    void shrink();

    inline decltype(prev_iterator) get_prev_iterator() const {
        return this->prev_iterator;
    }
};

}

#endif //_CONNECTION_H
