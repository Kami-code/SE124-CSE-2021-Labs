#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

long long int get_current_ms_time();

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    // Your code here
    request_vote_args() {}
    request_vote_args(int t, int c, int li, int lt):term(t), candidateId(c), lastLogIndex(li), lastLogTerm(lt) {};
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    // Your code here
    int term;
    bool voteGranted;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    // Your code here
    int received_term;
    command com;
    log_entry() {};
    log_entry(int received_term, command com): received_term(received_term), com(com) {};
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m << entry.received_term;
    m << entry.com;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u >> entry.received_term;
    u >> entry.com;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    append_entries_args() {}
    append_entries_args(int t, int li, int pli, int plt, std::vector<log_entry<command>> e, int lc):term(t), leaderId(li), prevLogIndex(pli), prevLogTerm(plt),
    entries(e), leaderCommit(lc) {};
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<log_entry<command>> entries;
    int leaderCommit;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m << args.term;
    m << args.leaderId;
    m << args.prevLogIndex;
    m << args.prevLogTerm;
    m << args.entries;
    m << args.leaderCommit;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u >> args.term;
    u >> args.leaderId;
    u >> args.prevLogIndex;
    u >> args.prevLogTerm;
    u >> args.entries;
    u >> args.leaderCommit;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &u, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
    int term;
    int leaderId;
    int lastIncludeIndex;
    int lastIncludeTerm;
    int offset;
    std::vector<char> data;
    bool done;
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &u, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
    int term;
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &u, install_snapshot_reply& reply);



#endif // raft_protocol_h