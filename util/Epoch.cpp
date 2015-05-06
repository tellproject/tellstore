#include "Epoch.hpp"

#include <array>

#include <jemalloc/jemalloc.h>

namespace {
struct lists {
    struct node {
        std::atomic<node*> next;
        void* const ptr;
        std::function<void()> destruct;

        node(void* p)
            : ptr(p),
              next(reinterpret_cast<node*>(0x1)) {
        }

        ~node() {
            destruct();
        }

        bool own(decltype(node::destruct) destruct) {
            while (true) {
                auto n = next.load();
                if (reinterpret_cast<node*>(0x1) != n) {
                    return false;
                }
                if (next.compare_exchange_strong(n, nullptr)) {
                    this->destruct = destruct;
                    return true;
                }
            }
        }
    };

    struct list {
        list()
            : head_(reinterpret_cast<node*>(0x0)) {
        }

        ~list() {
            node* head = head_.load();
            while (reinterpret_cast<uint64_t>(head) != 0x0) {
                node* next = head->next.load();
                auto ptr = head->ptr;
                head->~node();
                ::free(ptr);
                head = next;
            }
        }

        std::atomic<node*> head_;

        void append(uint8_t* ptr, decltype(node::destruct) destruct) {
            ptr -= sizeof(node);
            auto nd = reinterpret_cast<node*>(ptr);
            if (!nd->own(destruct)) return;
            do {
                node* head = head_.load();
                nd->next = head;
                if (head_.compare_exchange_strong(head, nd)) return;
            } while (true);
        }
    };

    std::array<list, tell::store::NUM_LISTS> lists_;

    void append(uint8_t* ptr, uint64_t mycnt, decltype(node::destruct) destruct) {
        lists_[mycnt % tell::store::NUM_LISTS].append(ptr, destruct);
    }
};

std::atomic<std::atomic<uint64_t>*> active_cnt;
std::atomic<std::atomic<uint64_t>*> old_cnt;
std::atomic<std::atomic<uint64_t>*> oldest_cnt;
std::atomic<lists*> active_list;
std::atomic<lists*> old_list;
std::atomic<lists*> oldest_list;
}

namespace tell {
namespace store {

void* malloc(std::size_t size) {
    return allocator::malloc(size);
}

void* malloc(std::size_t size, std::size_t align) {
    return allocator::malloc(size, align);
}

void free(void* ptr) {
    allocator::free(ptr);
}

void free_now(void* ptr) {
    allocator::free_now(ptr);
}

void init() {
    active_cnt.store(new std::atomic<uint64_t>(1));
    old_cnt.store(new std::atomic<uint64_t>(0));
    oldest_cnt.store(new std::atomic<uint64_t>(0));

    active_list.store(new lists());
    old_list.store(new lists());
    oldest_list.store(new lists());
}

void destroy() {
    delete oldest_list.load();
    delete old_list.load();
    delete active_list.load();

    delete oldest_cnt.load();
    delete old_cnt.load();
    delete active_cnt.load();
}

allocator::allocator() {
    do {
        cnt_ = active_cnt.load();
        auto my_cnt_ = cnt_->load();
        if (my_cnt_ % 2 && cnt_->compare_exchange_strong(my_cnt_, my_cnt_ + 2)) {
            return;
        }
    } while (true);
}

allocator::~allocator() {
    cnt_->fetch_sub(2);
    auto& oldcnt = *(old_cnt.load());
    auto& oldestcnt = *(oldest_cnt.load());
    auto& ac = *(active_cnt.load());
    uint64_t oac = ac.load();
    if (oldestcnt.load() == 0 && oldcnt.load() == 0 && oac % 2 == 1) {
        if (!ac.compare_exchange_strong(oac, oac - 1)) {
            return;
        }
        auto activecnt = active_cnt.load();
        active_cnt.store(oldest_cnt.load());
        oldest_cnt.store(old_cnt.load());
        old_cnt.store(activecnt);
        auto todelete = oldest_list.load();
        oldest_list.store(old_list.load());
        old_list.store(active_list.load());
        active_list.store(new lists());
        active_cnt.load()->fetch_add(1);
        delete todelete;
    }
}

void* allocator::malloc(std::size_t size) {
    uint8_t* res = reinterpret_cast<uint8_t*>(::malloc(size + sizeof(lists::node)));
    if (!res) {
        return nullptr;
    }

    new(res) lists::node(res);
    return res + sizeof(lists::node);
}

void* allocator::malloc(std::size_t size, std::size_t align) {
    size_t nodePadding = ((sizeof(lists::node) % align != 0) ? (align - (sizeof(lists::node) % align)) : 0);

    uint8_t* res = reinterpret_cast<uint8_t*>(::aligned_alloc(align, size + sizeof(lists::node) + nodePadding));
    if (!res) {
        return nullptr;
    }

    new(res + nodePadding) lists::node(res);
    return res + sizeof(lists::node) + nodePadding;
}

void allocator::free(void* ptr, std::function<void()> destruct) {
    unsigned long long int t;
    __asm__ volatile (".byte 0x0f, 0x31" : "=A" (t));
    active_list.load()->append(reinterpret_cast<uint8_t*>(ptr), t, destruct);
}

void allocator::free_now(void* ptr) {
    uint8_t* res = reinterpret_cast<uint8_t*>(ptr);
    res -= sizeof(lists::node);
    auto nd = reinterpret_cast<lists::node*>(res);
    ::free(nd->ptr);
}

} // namespace store
} // namespace tell
