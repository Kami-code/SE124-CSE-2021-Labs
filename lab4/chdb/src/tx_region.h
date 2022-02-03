#include "ch_db.h"

class operation {
public:
    enum oper {GET, PUT};
    oper oper_;
    int key;
    int value = -1;
    operation (oper oper_, int key, int value):oper_(oper_), key(key), value(value) {};
};


/*
 * tx_region: chdb KV client which supports transaction concurrency control.
 * */
class tx_region {
public:
    tx_region(chdb *db) : db(db),
                          tx_id(db->next_tx_id()) {
        this->tx_begin();
    }

    ~tx_region() {
        if (this->tx_can_commit() == chdb_protocol::prepare_ok) this->tx_commit();
        else this->tx_abort();
    }

    /**
     * Dummy request. Only for test
     * */
    int dummy();

    /**
     * Put one kv into the storage
     *
     * Note!: The changes in a the transaction region cannot be viewed by other regions until it commits.
     * Meanwhile, those changes can be **seen** in self region.
     * */
    int put(const int key, const int val);

    /**
     * Query one value from the storage by `key`
     * */
    int get(const int key);

    /**
     * Transaction check whether could commit.
     * Return 1 if all of the shards are ok to commit, and 0 if exists one not ok.
     * */
    int tx_can_commit();

    int init_locks(int key);

    int tx_restart();

    void release_all_locks();

    void check_access(int key);

    /*******************************************
     * Transaction part
     * ********************************************/
private:
    /**
     * Transaction begin
     * */
    int tx_begin();

    /**
     * Transaction commit. Sending `Commit` messages to all of the shard clients
    * */
    int tx_commit();

    /**
     * Transaction abort. Sending `Rollback` messages to all of the shard clients
     * */
    int tx_abort();

    chdb *db;
    const int tx_id;
    bool has_write = false;
    std::vector<operation> oper_list;
    bool no_write_into_oper_list = false;

};
