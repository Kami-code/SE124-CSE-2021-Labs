#include "raft_protocol.h"

long long int get_current_ms_time() {
    // https://www.thinbug.com/q/49090366
    std::chrono::time_point< std::chrono::system_clock > now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto milliseconds = std::chrono::duration_cast< std::chrono::milliseconds >( duration );
    long long int millisecounds_count = milliseconds.count();
    return millisecounds_count;
}

marshall& operator<<(marshall &m, const request_vote_args& args) {
    // Your code here
    m << args.term;
    m << args.candidateId;
    m << args.lastLogIndex;
    m << args.lastLogTerm;
    return m;

}
unmarshall& operator>>(unmarshall &u, request_vote_args& args) {
    // Your code here
    u >> args.term;
    u >> args.candidateId;
    u >> args.lastLogIndex;
    u >> args.lastLogTerm;
    return u;
}

marshall& operator<<(marshall &m, const request_vote_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.voteGranted;
    return m;
}

unmarshall& operator>>(unmarshall &u, request_vote_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.voteGranted;
    return u;
}

marshall& operator<<(marshall &m, const append_entries_reply& reply) {
    // Your code here
    m << reply.term;
    m << reply.success;
    return m;
}
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply) {
    // Your code here
    u >> reply.term;
    u >> reply.success;
    return u;
}

marshall& operator<<(marshall &m, const install_snapshot_args& args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.lastIncludeIndex;
    m << args.lastIncludeTerm;
    m << args.offset;
    m << args.data;
    m << args.done;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_args& args) {
    // Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.lastIncludeIndex;
    u >> args.lastIncludeTerm;
    u >> args.offset;
    u >> args.data;
    u >> args.done;
    return u; 
}

marshall& operator<<(marshall &m, const install_snapshot_reply& reply) {
    // Your code here
    m << reply.term;
    return m;
}

unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply) {
    // Your code here
    u >> reply.term;
    return u;
}