#include <list>
#include <cstdlib>
#include <cstring>
#include <string>

#include <ev.h>
#include <jemalloc.h>

#include <assoc.h>
#include <worker.h>
#include <setting.h>
#include <connection.h>

namespace cached {

static auto setting = setting::get_instance();

connection::connection(int fd, worker &w, std::list<connection>::iterator end) :
prev_iterator(--end),
state(connection::conn_state::WAIT_CMD),
sfd(fd),
r_size(setting.conn_read_buffer_size),
w_size(setting.conn_write_buffer_size),
r_unparsed(0),
w_unwrite(0),
ritem_buf_len(0),
ritem_buf(nullptr),
parse_state_curr(connection::cmd_parse_state::SWALLOW_SPACE),
next_parse_state(connection::cmd_parse_state::CMD_NAME),
worker_base(w),
wevent_bound(false)
{
    this->rbuf = new char[this->r_size];
    this->wbuf = new char[this->w_size];
    this->rcurr = this->rbuf;
    this->wcurr = this->wbuf;

    ev_io_init(&this->read_evio, connection::drive_machine, this->sfd, EV_READ);
    ev_io_start(this->worker_base.evloop, &this->read_evio);

    ev_io_init(&this->write_evio, connection::write_response, this->sfd, EV_WRITE);
}

void connection::shrink() {
    auto& setting = setting::get_instance();

    this->state = connection::conn_state::READ_CMD_BUF;
    this->parse_state_curr = cmd_parse_state::CMD_NAME;
    this->next_parse_state = cmd_parse_state::SWALLOW_SPACE;

    if (this->r_unparsed > 0) {
        std::memmove(this->rbuf, this->rcurr, this->r_unparsed);
    }
    this->rcurr = this->rbuf;

    if (this->r_unparsed < this->r_size / 2) {
        this->r_size = std::max(setting.conn_write_buffer_size, this->r_unparsed * 2);
        this->rbuf = static_cast<char *>(std::realloc(this->rbuf, this->r_size));
    }

    if (this->w_unwrite > 0) {
        std::memmove(this->wbuf, this->wcurr, this->w_unwrite);
    }
    this->wcurr = this->wbuf;

    if (this->w_unwrite < this->w_size / 2) {
        this->w_size = std::max(setting.conn_write_buffer_size, this->w_unwrite * 2);
        this->wbuf = static_cast<char *>(std::realloc(this->wbuf, this->w_size));
    }

}

void connection::drive_machine(EV_P_ ev_io *w, int revents) noexcept {
    auto& conn = connection::get_connection(w);

    int n_req = 25;
    bool stop = false;
    size_t unparsed_before;
    while (!stop) {
        switch (conn.state) {
            case conn_state::WAIT_CMD:
                if (--n_req <= 0) {
                    return;
                }

                conn.shrink();
                break;

            case conn_state::READ_CMD_BUF:
                unparsed_before = conn.r_unparsed;

                switch (conn.try_read_command()) {
                    case read_cmd_result::SUCCESS:
                        conn.state = conn_state::PARSE_CMD;
                        break;

                    case read_cmd_result::READ_ERROR:
                    case read_cmd_result::MEMORY_ERROR:
                        conn.worker_base.remove_conn(conn);
                        return;

                    case read_cmd_result::NOTHING:
                        if (conn.r_unparsed > unparsed_before) {
                            conn.state = conn_state::PARSE_CMD;
                        }
                        return;
                }
                break;

            case conn_state::PARSE_CMD:
                switch (conn.try_parse_command()) {
                    case cmd_parse_result::ERROR:
                        conn.worker_base.remove_conn(conn);
                        break;

                    case cmd_parse_result::BUF_EMPTY:
                        conn.state = conn_state::READ_CMD_BUF;
                        break;

                    case cmd_parse_result::FINISH:
                        conn.execute_command();
                        conn.state = conn_state::WAIT_CMD;
                        break;
                }
                break;

            default:
                break;
        }
    }
}

connection::read_cmd_result connection::try_read_command() noexcept {
    if (this->rcurr != this->rbuf) {
        if (this->r_unparsed > 0) {
            std::memmove(this->rbuf, this->rcurr, this->r_unparsed);
        }

        this->rcurr = this->rbuf;
    }

    auto n_realloc = 0;
    auto got_data = read_cmd_result::NOTHING;

    while (true) {
        auto remain = this->r_size - this->r_unparsed;
        if (remain == 0) {
            if (n_realloc == 4) {
                return read_cmd_result::SUCCESS;
            }

            n_realloc++;

            auto newbuf = static_cast<char *>(std::realloc(this->rbuf, this->r_size * 2));
            if (newbuf == NULL) {
                // TODO
                return read_cmd_result::MEMORY_ERROR;
            }

            remain = this->r_size;
            this->r_size *= 2;
            this->rcurr = newbuf + (this->rcurr - this->rbuf);
            this->rbuf = this->rcurr;
        }

        auto res = read(this->sfd,
                        this->rcurr + this->r_unparsed,
                        remain);

        if (res > 0) {
            this->r_unparsed += res;
            got_data = read_cmd_result::SUCCESS;
        } else if (res == 0) {
            return read_cmd_result::READ_ERROR;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return got_data;
            }
            return read_cmd_result::READ_ERROR;
        }
    }
}

// StorageCommand:
//    (set | add | replace | append | prepend) Key Flags Exptime ItemSize \r\n Item \r\n
//    cas Key Flags Exptime Bytes CasKey \r\n Bytes \r\n
//    delete Key \r\n
//
// RetrievalCommand:
//    (get | gets) Key+ \r\n
connection::cmd_parse_result connection::try_parse_command() noexcept {
    auto static const setting = setting::get_instance();

    if (this->r_unparsed == 0) {
        return cmd_parse_result::BUF_EMPTY;
    }

    size_t count;
    char *end, *cr, *dest;
    std::string key_curr;
    cmd_parse_result np_result;

    while (this->r_unparsed > 0 || this->parse_state_curr == cmd_parse_state::SUCCESS)
    {
        switch (this->parse_state_curr) {
            case cmd_parse_state::CMD_NAME:
                this->next_parse_state = cmd_parse_state::KEY;

                if (this->r_unparsed >= 4) {
#define V(cmdstr, cmdenum)\
                    if (std::strncmp(this->rcurr, cmdstr, std::strlen(cmdstr)) == 0) {\
                        this->r_unparsed -= std::strlen(cmdstr);\
                        this->rcurr += std::strlen(cmdstr);\
                        this->cmd_curr = cmdenum;\
                        goto cmd_parse_success;\
                    }
                    FOREACH_COMMAND(V)
#undef V

                    return cmd_parse_result::ERROR;

                    cmd_parse_success:
                    this->parse_state_curr = cmd_parse_state::SWALLOW_SPACE;
                    this->cmd_key.clear();
                } else {
                    return cmd_parse_result::BUF_EMPTY;
                }
                break;

            case cmd_parse_state::KEY:
                if (this->cmd_curr == cmd_type::GET
                    || this->cmd_curr == cmd_type::DELETE
                    || this->cmd_curr == cmd_type::GETS)
                {
                    this->next_parse_state = cmd_parse_state::KEY;
                } else {
                    this->next_parse_state = cmd_parse_state::FLAG;
                }

                while (this->r_unparsed > 0 && *this->rcurr != ' ') {
                    if (this->rcurr[0] == '\r'
                        && this->r_unparsed > 1
                        && this->rcurr[1] == '\n')
                    {
                        if (this->cmd_curr == cmd_type::GET
                            || this->cmd_curr == cmd_type::GETS)
                        {
                            if (key_curr.size() > 0) {
                                this->cmd_key.push_back(key_curr);
                                this->cmd_key.shrink_to_fit();
                                key_curr.clear();
                            }

                            this->parse_state_curr = cmd_parse_state::SWALLOW_NEW_LINE;
                            this->next_parse_state = cmd_parse_state::SUCCESS;
                            break;
                        } else {
                            return cmd_parse_result::ERROR;
                        }
                    }

                    key_curr.append(this->rcurr++, 1);
                    this->r_unparsed--;
                }

                if (key_curr.size() > setting.max_key_len) {
                    return cmd_parse_result::ERROR;
                }

                if (this->r_unparsed > 0 && *this->rcurr == ' ') {
                    this->parse_state_curr = cmd_parse_state::SWALLOW_SPACE;
                    this->cmd_key.push_back(key_curr);
                    this->cmd_key.shrink_to_fit();
                    key_curr.clear();
                }

                break;

            case cmd_parse_state::FLAG:
                this->next_parse_state = cmd_parse_state::EXPTIME;

                if ((np_result = this->try_parse_number(this->cmd_flag))
                    == cmd_parse_result::FINISH) {
                    this->parse_state_curr = cmd_parse_state::SWALLOW_SPACE;
                } else {
                    return np_result;
                }
                break;

            case cmd_parse_state::EXPTIME:
                if (this->cmd_curr == cmd_type::CAS) {
                    this->next_parse_state = cmd_parse_state::CAS_KEY;
                } else {
                    this->next_parse_state = cmd_parse_state::ITEM_SIZE;
                }

                if ((np_result = this->try_parse_number(this->cmd_exptime))
                    == cmd_parse_result::FINISH) {
                    this->parse_state_curr = cmd_parse_state::SWALLOW_SPACE;
                } else {
                    return np_result;
                }

                break;

            case cmd_parse_state::CAS_KEY:
                this->next_parse_state = cmd_parse_state::ITEM_SIZE;

                if ((np_result = this->try_parse_number(this->cmd_cas_key))
                    == cmd_parse_result::FINISH) {
                    this->parse_state_curr = cmd_parse_state::SWALLOW_SPACE;
                } else {
                    return np_result;
                }

                break;

            case cmd_parse_state::ITEM_SIZE:
                this->next_parse_state = cmd_parse_state::ITEM;

                if ((np_result = this->try_parse_number(this->cmd_item_size))
                    == cmd_parse_result::FINISH)
                {
                    if (this->cmd_item_size > setting.max_item_size) {
                        return cmd_parse_result::ERROR;
                    } else {
                        this->parse_state_curr = cmd_parse_state::SWALLOW_NEW_LINE;

                        this->ritem_saved = 0;
                        if (this->cmd_item_size != this->ritem_buf_len) {
                            this->ritem_buf =
                                    static_cast<char *>(
                                            std::realloc(this->ritem_buf,
                                                         this->cmd_item_size));
                            if (this->ritem_buf == NULL) {
                                return cmd_parse_result::ERROR;
                            }

                            this->ritem_buf_len = this->cmd_item_size;
                        }
                    }
                } else {
                    return np_result;
                }
                break;


            case cmd_parse_state::ITEM:
                this->next_parse_state = cmd_parse_state::SUCCESS;

                end = this->rcurr + this->r_unparsed - 1;
                cr = static_cast<char *>(std::memchr(this->rcurr, '\r',
                                                           this->r_unparsed));
                if (cr == NULL) {
                    count = this->r_unparsed;
                } else if (end - cr > 0) {
                    if (cr[1] == '\n') {
                        count = cr - this->rcurr;
                        this->parse_state_curr = this->next_parse_state;
                    } else {
                        count = (cr + 1) - this->rcurr + 1;
                    }
                } else if (cr == this->rcurr) {
                    return cmd_parse_result::BUF_EMPTY;
                } else { // cr == this->rcurr + this->r_unparsed - 1
                    count = cr - this->rcurr;
                }

                if (this->ritem_saved + count > this->ritem_buf_len) {
                    return cmd_parse_result::ERROR;
                }

                std::memmove(this->ritem_buf + this->ritem_saved, this->rcurr, count);
                this->ritem_saved += count;

                this->r_unparsed -= count;

                if (this->parse_state_curr != cmd_parse_state::SUCCESS) {
                    this->rcurr += count;
                } else {
                    this->rcurr += count + 2;
                    this->r_unparsed -= 2;
                }

                break;

            case cmd_parse_state::SWALLOW_NEW_LINE:
                if (this->r_unparsed > 1) {
                    if (this->rcurr[0] == '\r' && this->rcurr[1] == '\n') {
                        this->rcurr += 2;
                        this->r_unparsed -= 2;

                        this->parse_state_curr = this->next_parse_state;
                    } else {
                        return cmd_parse_result::ERROR;
                    }
                } else {
                    return cmd_parse_result::BUF_EMPTY;
                }

                break;

            case cmd_parse_state::SWALLOW_SPACE:
                while (this->r_unparsed > 0 && *this->rcurr == ' ') {
                    this->rcurr++;
                    this->r_unparsed--;
                }

                if (this->r_unparsed < 1) {
                    return cmd_parse_result::BUF_EMPTY;
                }

                if (*this->rcurr != ' ') {
                    this->parse_state_curr = this->next_parse_state;
                }

                break;

            case cmd_parse_state::SUCCESS:
                return cmd_parse_result::FINISH;
        }
    }

    return cmd_parse_result::BUF_EMPTY;
}

void connection::execute_command() noexcept {
    static auto &hash_table = hash_table::get_instance();

    item_ptr it;
    bucket * bp;
    if (this->cmd_curr == cmd_type::GET) {
        this->execute_get(false);
    } else if (this->cmd_curr == cmd_type::GETS) {
        this->execute_gets();
    } else if (this->cmd_curr == cmd_type::DELETE) {
        this->execute_delete();
    } else {
        it = hash_table.find_item(this->cmd_key[0], bp);
        if (!it) {
            if (this->cmd_curr == cmd_type::SET
                || this->cmd_curr == cmd_type::ADD)
            {
                this->execute_add();
            } else {
                this->wbuf_append("NOT_FOUND\r\n");
            }

            return;
        }

        if (this->cmd_curr == cmd_type::CAS) {
            this->execute_cas(it, bp);
            return;
        }

        it->update_cas_key();

        switch (this->cmd_curr) {
            case cmd_type::SET:
            case cmd_type::REPLACE:
                this->execute_replace(it, bp);
                break;

            case cmd_type::PREPEND:
                this->execute_prepend_or_append(it, bp, false);
                break;

            case cmd_type::APPEND:
                this->execute_prepend_or_append(it, bp, true);
                break;

            case cmd_type::ADD:
                this->wbuf_append("EXISTS\r\n");
                break;

            default:
                break;
        }
    }
}

void connection::execute_get(bool return_cas) noexcept {
    static auto &hash_table = hash_table::get_instance();

    bool found = false;
    bucket *bp;
    item_ptr it;

    // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
    char buf[5 + 1 + 250 + 1 + 10 + 1 + 10 + 1 + 20 + 2 + 1];

    for (auto& key : this->cmd_key) {
        if ((it = hash_table.find_item(key, bp))) {
            found = true;

            if (return_cas) {
                std::sprintf(buf,
                             "VALUE %s %d %ld %lld\r\n",
                             it->key.c_str(),
                             it->flags,
                             it->data_size,
                             it->cas_key);
            } else {
                std::sprintf(buf,
                             "VALUE %s %d %ld\r\n",
                             it->key.c_str(),
                             it->flags,
                             it->data_size);
            }

            this->wbuf_append(buf, std::strlen(buf));
            this->wbuf_append(it->data, it->data_size);
            this->wbuf_append("\r\n");

            bp->unlock();
        }
    }

    if (found) {
        this->wbuf_append("END\r\n");
    } else {
        this->wbuf_append("NOT_FOUND\r\n");
    }
}

void connection::execute_delete() noexcept {
    static auto &hash_table = hash_table::get_instance();
    static auto &lru_queue = lru_queue::get_instance();

    bucket *bp;
    item_ptr it;
    char buf[259];

    for (auto &key : this->cmd_key) {
        if ((it = hash_table.find_item(key, bp, false))) {
            std::sprintf(buf, "DELETED %s\r\n", it->key.c_str());

            bp->remove(it);
            bp->unlock();

            lru_queue.remove_with_lock(it);

            this->wbuf_append(buf, 10 + it->key.size());
        }
    }
}

void connection::execute_cas(item_ptr &it, bucket *&bp) noexcept {
    if (this->cmd_cas_key == it->cas_key) {
        it->update_cas_key();

        auto new_data = je_realloc(it->data, this->ritem_buf_len);
        if (!new_data) {
            this->wbuf_append("ERROR\r\n");
        } else {
            std::memmove(it->data, this->ritem_buf, this->ritem_buf_len);
            it->data_size = this->ritem_buf_len;
        }
    } else {
        this->wbuf_append("NOT_STORED\r\n");
    }

    bp->unlock();
}

void connection::execute_prepend_or_append(item_ptr &it, bucket *&bp, bool append)
noexcept
{
    auto new_data = je_realloc(this->ritem_buf, this->ritem_buf_len + it->data_size);

    if (!new_data) {
        this->wbuf_append("ERROR\r\n");
    } else {
        if (append) {
            std::memmove(it->data + it->data_size, this->ritem_buf, this->ritem_buf_len);
        } else {
            std::memmove(it->data + this->ritem_buf_len, it->data, it->data_size);
            std::memmove(it->data, this->ritem_buf, this->ritem_buf_len);
        }

        it->data_size += this->ritem_buf_len;
    }

    bp->unlock();
}

void connection::execute_replace(item_ptr &it, bucket *&bp) noexcept {
    auto new_data = je_realloc(it->data, this->ritem_buf_len);

    if (!new_data) {
        this->wbuf_append("ERROR\r\n");
    } else {
        std::memmove(it->data, this->ritem_buf, this->ritem_buf_len);
        it->data_size = this->ritem_buf_len;

        this->wbuf_append("STORED\r\n");
    }

    bp->unlock();
}

void connection::execute_add() noexcept {
    static auto &hash_table = hash_table::get_instance();

    hash_table.insert_item(this->cmd_key[0],
                           this->cmd_flag,
                           this->cmd_exptime,
                           this->ritem_buf,
                           this->ritem_buf_len);

    this->wbuf_append("STORED\r\n");
}

void connection::wbuf_append(const char *buf, size_t size) noexcept {
    if (this->wcurr > this->wbuf) {
        std::memmove(this->wbuf, this->wcurr, this->w_size);
        this->wcurr = this->wbuf;
    }

    if (this->w_unwrite + size > this->w_size) {
        auto new_ptr = std::realloc(this->wbuf, this->w_size + size);
        if (!new_ptr) {
            return;
        }
        this->wbuf = static_cast<char *>(new_ptr);
    }

    std::memcpy(this->wbuf + this->w_unwrite, buf, size);
    this->w_unwrite += size;

    if (!this->wevent_bound) {
        write_again:

        auto nwrite = write(this->sfd, this->wcurr, this->w_unwrite);
        if (nwrite > 0) {
            this->w_unwrite -= nwrite;
            if (this->w_unwrite > 0) {
                goto write_again;
            }
        } else {
            ev_io_start(this->worker_base.evloop, &this->write_evio);
            this->wevent_bound = true;
        }
    }
}

void connection::write_response(EV_P_ ev_io *w, int revents) noexcept {
    static const auto offset = reinterpret_cast<uint64_t>(&((connection *)0)->write_evio);

    auto conn = reinterpret_cast<connection *>((uint64_t)(w) - offset);

    while (conn->w_unwrite > 0) {
        auto nwrite = write(conn->sfd, conn->wbuf, conn->w_unwrite);
        if (nwrite > 0) {
            conn->w_unwrite -= nwrite;
            conn->wcurr += nwrite;

            if (conn->w_unwrite <= 0) {
                ev_io_stop(conn->worker_base.evloop, w);
                conn->wevent_bound = false;
            }
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        } else {
            perror("failed to write response");
        }
    }
}

connection::~connection() {
    close(this->sfd);
    delete this->rbuf;
    delete this->wbuf;
}

}