#ifndef _ASSOC_H
#define _ASSOC_H

#include <cstdlib>
#include <memory>
#include <atomic>
#include <string>
#include <mutex>
#include <list>

#include <stdint.h>
#include <murmur3.h>

namespace cached {

class item;

typedef std::shared_ptr<item> item_ptr;
typedef std::weak_ptr<item> item_weak_ptr;
typedef std::lock_guard<std::mutex> mtx_guard;

class bucket {
private:
    std::mutex mtx;
    item_ptr head = nullptr;

public:
    friend class item;
    friend class hash_table;

    void insert_item(item_ptr &it);
    void remove(item_ptr &it);

    inline void unlock() {
        this->mtx.unlock();
    }
};

class hash_table {
public:
    friend class item;

    static hash_table& get_instance() {
        static hash_table instance;
        return instance;
    }

    hash_table(const hash_table & a) = delete;
    hash_table & operator=(const hash_table & a) = delete;

    inline uint32_t hash(std::string &key) noexcept {
        uint32_t res;
        MurmurHash3_x86_32(key.c_str(),
                           static_cast<int>(key.size()),
                           this->hash_seed,
                           &res);
        return res;
    }

    uint32_t inline hash_index(uint32_t hv) noexcept {
        return hv % (1 << power);
    }

    inline bucket& get_bucket(uint32_t index) noexcept {
        return this->table[index];
    }

    inline bucket& get_bucket(std::string& key) noexcept {
        return this->get_bucket(this->hash_index(this->hash(key)));
    }

    item_ptr insert_item(std::string &key, uint32_t flags,
                             unsigned int exptime, char *data,
                             size_t data_size);

    item_ptr find_item(std::string &key, bucket *&bp, bool update_lru = true);


    bool inline is_expanding() const noexcept {
        return this->expanding;
    }

private:
    size_t nitems;

    bucket *table;
    std::mutex table_lock;

    bool expanding;

    unsigned int power = 10;

    uint32_t hash_seed;

    hash_table();
};

class lru_queue {
    item_ptr head;
    item_ptr tail;
    size_t length;
    std::atomic<size_t> item_total_size;

    std::mutex mtx;

    lru_queue();

public:
    friend class hash_table;

    static lru_queue& get_instance() noexcept {
        static lru_queue instance;
        return instance;
    }

    lru_queue(const lru_queue& l) = delete;
    lru_queue & operator=(const lru_queue& l) = delete;

    void move_head(item_ptr &it) noexcept;

    void remove(item_ptr &it) noexcept;

    inline void remove_with_lock(item_ptr &it) noexcept {
        mtx_guard g(this->mtx);
        this->remove(it);
    };
};

class item {
public:
    std::string key;
    char *data;
    time_t exptime;
    time_t created_at;
    time_t last_access;

    uint32_t flags;
    uint64_t cas_key;
    size_t data_size;

    uint32_t hash_index;

    item_weak_ptr lru_prev;
    item_ptr lru_next;
    item_weak_ptr hash_prev;
    item_ptr hash_next;

    ~item();

    item(std::string& key,
         uint32_t flags,
         unsigned int exptime,
         char * data,
         size_t data_size);

    inline void update_cas_key() noexcept {
        this->cas_key++;
    }
};

}

#endif //_ASSOC_H
