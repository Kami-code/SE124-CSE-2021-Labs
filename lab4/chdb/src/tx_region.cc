#include "tx_region.h"

/**
 * Dummy request. Only for test
 * */

int tx_region::init_locks(int key) {
    chdb *chdb_ = this->db;
    if (chdb_->entry_locks.count(key) == 0) {
        chdb_->entry_locks[key] = new std::mutex();
    }
    if (chdb_->hold_transaction.count(key) == 0) {
        chdb_->hold_transaction[key] = -1;
    }
    if (chdb_->key_locks.count(key) == 0) {
        chdb_->key_locks[key] = new std::mutex();
    }
}

void tx_region::release_all_locks() {
    chdb *chdb_ = this->db;
    for (auto &entry : chdb_->key_locks) {
        int key = entry.first; std::mutex *mtx = entry.second;
        int flag = chdb_->entry_locks[key]->try_lock();        // block others into the critical zone, so there maybe one thread waiting the mtx here
        if (chdb_->hold_transaction[key] == this->tx_id) {
            chdb_->hold_transaction[key] = -1;       // set the mutex is free, if there is someone waiting, he is in the critical zone now, will immediately change hold_transaction[key]
            mtx->unlock();
            printf("tx[%d] release the entry_lock[%d]\n", this->tx_id, key);
        }
        if (flag) chdb_->entry_locks[key]->unlock();
    }
}

int tx_region::dummy() {
    int r;
    this->db->vserver->execute(1,
                               chdb_protocol::Dummy,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = 1024, .value = 16},
                               r);
    return r;
}

int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    chdb *chdb_ = this->db;
    init_locks(key);
    if (!no_write_into_oper_list) {
        oper_list.push_back(operation(operation::PUT, key, val));
    }

    printf("in put, tx[%d] wait for the entry_lock[%d]\n", this->tx_id, key);
//    int flag = 0;
//    for (int i = 0; i < 100; i++) {
//        flag = chdb_->entry_locks[key]->try_lock();
//        if (flag) break;
//        else mssleep(20);
//    }
//    if (!flag) {
//        tx_abort();
//        mssleep(rand() % 500 + 200);
//        tx_restart();
//        return 0;
//    }
    chdb_->entry_locks[key]->lock();
    check_access(key);


    printf("tx[%d] put key = [%d], value = [%d]\n", tx_id, key, val);
    int shard_offset = db->vserver->dispatch(key, db->shards.size());
    int r;
    this->db->vserver->execute(shard_offset,
                               chdb_protocol::Put,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = val},
                               r);
    has_write = true;
    chdb_->entry_locks[key]->unlock();
    return r;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    chdb *chdb_ = this->db;
    init_locks(key);
    if (!no_write_into_oper_list) {
        oper_list.push_back(operation(operation::GET, key, -1));
    }

    printf("in get tx[%d] wait for the entry_lock[%d]\n", this->tx_id, key);
//    int flag = 0;
//    for (int i = 0; i < 100; i++) {
//        flag = chdb_->entry_locks[key]->try_lock();
//        if (flag) break;
//        else mssleep(20);
//    }
//    if (!flag) {
//        tx_abort();
//        mssleep(rand() % 500 + 200);
//        tx_restart();
//    }
    chdb_->entry_locks[key]->lock();
    check_access(key);

    printf("tx[%d] get key = [%d]\n", tx_id, key);

    int shard_offset = db->vserver->dispatch(key, db->shards.size());
//    std::cout << "key = " << key << " shard_size = " << db->shards.size() << " shard_offset = " << shard_offset << std::endl;
    int r;
    chdb_->vserver->execute(shard_offset,
                               chdb_protocol::Get,
                               chdb_protocol::operation_var{.tx_id = tx_id, .key = key, .value = 0},
                               r);

    chdb_->entry_locks[key]->unlock();
    return r;
}

void tx_region::check_access(int key) {
    chdb *chdb_ = this->db;
    int &hold_tx = chdb_->hold_transaction[key];
    if (hold_tx == this->tx_id) {
        // the transaction is holding the lock
    }
    else if (hold_tx == -1) {
        // no one is using the lock
        if (chdb_->key_locks[key]->try_lock()) {
            // the transaction is holding the lock
            printf("tx[%d] get the key_lock[%d]\n", this->tx_id, key);
            hold_tx = this->tx_id;
        }
        else {
            // between the interim, someone has taken the lock.
//            assert(0);
            chdb_->entry_locks[key]->unlock();
            tx_abort();
            mssleep(rand() % 500 + 200);
            tx_restart();
            printf("check_access tx[%d] wait for the entry_lock[%d]\n", this->tx_id, key);
            chdb_->entry_locks[key]->lock();
        }
    }
    else {
        if (this->tx_id < hold_tx) {
            // current tx is older, should wait here
            chdb_->entry_locks[key]->unlock();
            printf("i am older, tx[%d] wait for the key_lock[%d]\n", this->tx_id, key);
            chdb_->key_locks[key]->lock();

            chdb_->entry_locks[key]->lock();
            printf("tx[%d] get the key_lock[%d]\n", this->tx_id, key);
            hold_tx = this->tx_id;
        }
        else {
            // current tx is younger, die and remake
            chdb_->entry_locks[key]->unlock();
            tx_abort();
            mssleep(rand() % 500 + 200);
            tx_restart();
            printf("check_access tx[%d] wait for the entry_lock[%d]\n", this->tx_id, key);
            chdb_->entry_locks[key]->lock();
        }
    }
}

int tx_region::tx_restart() {
    has_write = false;
    no_write_into_oper_list = true;
    int r;
    for (int i = 0; i < oper_list.size(); i++) {
        operation oper = oper_list[i];
        switch (oper.oper_) {
            case oper.GET:
                r = get(oper.key);
                break;
            case oper.PUT:
                r = put(oper.key, oper.value);
                break;
        }
    }
    no_write_into_oper_list = false;
    return r;
}



int tx_region::tx_can_commit() {
    if (!has_write) return chdb_protocol::prepare_ok;
    bool r = this->db->vserver->check_shard_prepared(tx_id);
    if (r) return chdb_protocol::prepare_ok;
    else return chdb_protocol::prepare_not_ok;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    printf("tx[%d] commit\n", tx_id);
    int r;
    for (int i = 1; i <= db->shards.size(); i++) {
        this->db->vserver->execute(i,
                                   chdb_protocol::Commit,
                                   chdb_protocol::commit_var{.tx_id = tx_id},
                                   r);
    }
    release_all_locks();
    return r;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    printf("tx[%d] abort\n", tx_id);
    int r;

    for (int i = 1; i <= db->shards.size(); i++) {
        this->db->vserver->execute(i,
                                   chdb_protocol::Rollback,
                                   chdb_protocol::rollback_var{.tx_id = tx_id},
                                   r);
    }

    release_all_locks();

    return r;
}
