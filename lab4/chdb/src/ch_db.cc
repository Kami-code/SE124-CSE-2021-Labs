#include "ch_db.h"

int view_server::execute(unsigned int shard_offset, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    int base_port = this->node->port();
    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute(unsigned int shard_offset, unsigned int proc, const chdb_protocol::commit_var &var, int &r) {
    int base_port = this->node->port();
    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute(unsigned int shard_offset, unsigned int proc, const chdb_protocol::rollback_var &var, int &r) {
    int base_port = this->node->port();
    return this->node->template call(base_port + shard_offset, proc, var, r);
}

bool view_server::check_shard_prepared(int tx_id) {
    int base_port = this->node->port();
    for (int shard_offset = 1; shard_offset <= shard_num(); shard_offset++) {
        int r = 0;
        int call_status = this->node->template call(base_port + shard_offset, chdb_protocol::CheckPrepareState, chdb_protocol::check_prepare_state_var{.tx_id = tx_id}, r);
        if (call_status != 0 || r != 1) return false;
    }
    return true;
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}