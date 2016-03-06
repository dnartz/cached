#include <utility>
#include <cstdlib>
#include <ctime>

#include <assoc.h>

#include <jemalloc.h>

namespace cached {

hash_table::hash_table() :
hash_seed(static_cast<uint32_t>(std::rand())),
power(10) {
    this->table = new bucket[1 << this->power];
}

lru_queue::lru_queue() :
length(0),
item_total_size(0),
head(nullptr),
tail(nullptr)
{ }

void lru_queue::move_head(item_ptr& it) noexcept {
    auto prev = it->lru_prev.lock();
    auto next = it->lru_next;

    if (!prev && !next) {
        this->item_total_size += it->data_size;
    }

    if (prev) {
        prev->lru_next = next;
    }

    if (next) {
        next->lru_prev = prev;
    }

    it->lru_next = this->head;
    it->lru_prev = item_weak_ptr();

    if (this->head) {
        this->head->lru_prev = it;
    }

    this->head = it;
}

void lru_queue::remove(item_ptr& it) noexcept {
    auto prev = it->lru_prev.lock();
    auto next = it->lru_next;

    if (prev) {
        prev->lru_next = next;
    }

    if (next) {
        next->lru_prev = prev;
    }

    if (it == this->head) {
        this->head = nullptr;
    }

    it->lru_next = nullptr;
    it->lru_prev.reset();
}

void bucket::insert_item(item_ptr &it) {
    it->hash_next = this->head;
    if (this->head) {
        this->head->hash_prev = it;
    }
    this->head = it;
}

void bucket::remove(item_ptr& it) {
    auto prev = it->hash_prev.lock();
    auto next = it->hash_next;

    if (prev) {
        prev->hash_next = next;
    }

    if (next) {
        next->hash_prev = prev;
    }

    if (it == this->head) {
        this->head = nullptr;
    }

    it->hash_next = nullptr;
    it->hash_prev.reset();
}

item::item(std::string &k,
           uint32_t flags,
           unsigned int exptime,
           char * new_data,
           size_t data_size) :
key(std::move(k)),
flags(flags),
exptime(exptime),
created_at(std::time(0)),
cas_key(0),
data((char *)je_malloc(data_size)),
data_size(data_size),
last_access(std::time(0))
{
    static auto& hash_table = hash_table::get_instance();

    this->hash_index = hash_table.hash_index(hash_table.hash(this->key));
    std::memcpy(this->data, new_data, data_size);
}

item_ptr hash_table::insert_item(std::string &key,
                                 uint32_t flags,
                                 unsigned int exptime,
                                 char *data,
                                 size_t data_size)
{
    static auto& lru = lru_queue::get_instance();
    static auto& hash_table = hash_table::get_instance();

    auto it = std::make_shared<item>(key, flags, exptime, data,data_size);

    if (it->exptime > 0) {
        mtx_guard g(lru.mtx);
        lru.move_head(it);
    }

    auto& bucket = this->get_bucket(it->hash_index);

    mtx_guard g(bucket.mtx);
    bucket.insert_item(it);

    return it;
}

item_ptr hash_table::find_item(std::string &key, bucket *&bp, bool update_lru) {
    static auto& lru_queue = lru_queue::get_instance();

    auto &bucket = this->get_bucket(key);
    bp = &bucket;

    bucket.mtx.lock();
    auto it = bucket.head;

    while (it) {
        if (it->key == key) {
            if (update_lru) {
                mtx_guard g1(lru_queue.mtx);
                lru_queue.move_head(it);
            }

            return it;
        }

        it = it->hash_next;
    }

    bucket.mtx.unlock();
    return nullptr;
}

item::~item() {
    je_free(this->data);
}

}