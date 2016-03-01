#include <list>
#include <cstdlib>
#include <cstring>
#include <string>

#include <ev.h>

#include <worker.h>
#include <setting.h>
#include <connection.h>

namespace cached {

static auto setting = setting::get_instance();

connection::connection(int fd, worker &w, std::list<connection>::iterator end) :
prev_iterator(--end),
state(connection::conn_state::WAIT_CMD),
sfd(fd),
r_size(setting.cl_read_buffer_size),
w_size(setting.cl_read_buffer_size),
r_unparsed(0),
ritem_buf_len(0),
ritem_buf(nullptr),
parse_state_curr(connection::cmd_parse_state::SWALLOW_SPACE),
next_parse_state(connection::cmd_parse_state::CMD_NAME),
worker_base(w)
{
    this->rbuf = new char[this->r_size];
    this->wbuf = new char[this->w_size];
    this->rcurr = this->rbuf;
    this->wcurr = this->wbuf;

    ev_io_init(&this->evio, connection::drive_machine, this->sfd, EV_READ);
    ev_io_start(this->worker_base.evloop, &this->evio);
}

void connection::shrink() {
    this->state = connection::conn_state::READ_CMD_BUF;
    this->parse_state_curr = cmd_parse_state::CMD_NAME;

    this->rcurr = this->rbuf;
    this->r_unparsed = 0;
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
                        // TODO
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

    while (this->r_unparsed > 0) {
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

                        this->ritem_used = 0;
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
                    } else {
                        count = (cr + 1) - this->rcurr + 1;
                    }
                } else if (cr == this->rcurr) {
                    return cmd_parse_result::BUF_EMPTY;
                } else { // cr == this->rcurr + this->r_unparsed - 1
                    count = cr - this->rcurr;
                }

                this->ritem_used += count;
                this->r_unparsed -= count;
                if (this->ritem_used > this->ritem_buf_len) {
                    return cmd_parse_result::ERROR;
                }

                dest = this->ritem_buf + this->ritem_buf_len;
                std::memmove(dest, this->rcurr, count);
                this->parse_state_curr = this->next_parse_state;

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

connection::~connection() {
    close(this->sfd);
    delete this->rbuf;
    delete this->wbuf;
}

}