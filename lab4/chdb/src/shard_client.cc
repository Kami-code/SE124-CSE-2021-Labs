#include "shard_client.h"
int shard_client::dummy(chdb_protocol::operation_var var, int &r) {
//    printf("Receive dummy Request! tx id:%d\n", var.tx_id);
    r = var.tx_id;
    return 0;
}

/*
class operation_var {
    public:
        int tx_id;
        int key;
        int value;
};
 */

int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
//    printf("shard_id = %d, Receive put Request! tx id:%d\n", shard_id, var.tx_id);
    int key = var.key;
    int id = var.tx_id;
    r = var.tx_id;
    temp_store[var.tx_id][var.key] = value_entry(var.value);
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
//    printf("shard_id = %d, Receive get Request! tx id:%d\n", shard_id, var.tx_id);

    if (temp_store[var.tx_id].count(var.key) != 0) {
        r = temp_store[var.tx_id][var.key].value;
    }
    else if (store[primary_replica].count(var.key) != 0) {
        r = store[primary_replica][var.key].value;
    }
    else {
        r = -1;
    }
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
//    printf("shard_id = %d, Receive commit Request! tx id:%d\n", shard_id, var.tx_id);
    for(auto & written_pair : temp_store[var.tx_id]) {
        int written_key = written_pair.first;
        value_entry written_entry = written_pair.second;
        for (auto &each_store : store) {
            each_store[written_key] = written_entry;
        }
//        store[0][written_key] = written_entry;
//        std::cout << "written_key = " << written_key << " written_value = " << written_entry.value << std::endl;
    }
    temp_store[var.tx_id].clear();
    r = 1;
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    temp_store[var.tx_id].clear();
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
//    printf("shard_id = %d, Receive check_prepare_state Request! tx id:%d\n", shard_id, var.tx_id);
    r = (int)active;
//    printf("state = %d\n", r);
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    r = 1;
    return 0;
}

shard_client::~shard_client() {
    delete node;
}