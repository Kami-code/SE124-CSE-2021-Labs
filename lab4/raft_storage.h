#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <stdio.h>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here

    void save_to_local();

    std::string file_path;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role = follower;
    int current_term = 0;
    int voteFor = -1; //candidateId that received vote in current term (or null if none), -1 represents null
    long long int last_received_RPC_time = -1; //the time we received the last RPC.

    int commitIndex = 0;
    int lastApplied = 0;

    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

    std::vector<log_entry<command>> logs;

    void set_role(int r) {
        std::unique_lock<std::mutex> lock(mtx);
        role = (raft_role)r;
    }
    void set_current_term(int ct) {
        std::unique_lock<std::mutex> lock(mtx);
        current_term = ct;
    }
    void set_vote_for(int vf) {
        std::unique_lock<std::mutex> lock(mtx);
        voteFor = vf;
    }
    void set_last_received_RPC_time(long long int lrRt) {
        std::unique_lock<std::mutex> lock(mtx);
        last_received_RPC_time = lrRt;
    }
    void set_commit_index(int ci) {
        std::unique_lock<std::mutex> lock(mtx);
        commitIndex = ci;
    }
    void set_last_applied(int la) {
        std::unique_lock<std::mutex> lock(mtx);
        lastApplied = la;
    }
    void set_next_index(std::vector<int> ni) {
        std::unique_lock<std::mutex> lock(mtx);
        nextIndex = ni;
    }
    void set_match_index(std::vector<int> mi) {
        std::unique_lock<std::mutex> lock(mtx);
        matchIndex = mi;
    }
    void set_logs(std::vector<log_entry<command>> l) {
        std::unique_lock<std::mutex> lock(mtx);
        logs = l;
    }


private:
    std::mutex mtx;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
//    std::cout << "loading raft_storage" << std::endl;
    std::unique_lock<std::mutex> lock(mtx);
    file_path = dir + std::string("/log.txt");
    std::ifstream in(file_path);
    if (!in.is_open()) return;
    int role_int;
    in >> role_int >> current_term >> voteFor >> last_received_RPC_time >> commitIndex >> lastApplied;
    role = (raft_role)role_int;
    int nextIndexVectorSize, matchIndexVectorSize;
    in >> nextIndexVectorSize >> matchIndexVectorSize;
//    std::cout << "nextIndexVectorSize" << nextIndexVectorSize << " matchIndexVectorSize" << matchIndexVectorSize << std::endl;
    for(int i = 0; i < nextIndexVectorSize; i++) {
        int tmp; in >> tmp;
        nextIndex.push_back(tmp);
    }
    for(int i = 0; i < matchIndexVectorSize; i++) {
        int tmp; in >> tmp;
        matchIndex.push_back(tmp);
    }
    int logs_size; in >> logs_size;
    std::cout << "loading logs" << std::endl;
    for (int i = 0; i < logs_size; i++) {
        int line_size, received_term;
        in >> line_size >> received_term;
        command cmd;
        char* com_string = (char*)malloc(sizeof(char) * line_size);

        for (int j = 0; j < line_size; j++) {
            int tmp; in >> tmp;
            com_string[j] = (char)tmp;
        }
//        std::cout << (int)(com_string[0]) << " " << (int)(com_string[1]) << " " << (int)(com_string[2]) << " " << (int)(com_string[3]) << std::endl;
        cmd.deserialize(com_string, line_size);

        std::cout << "[" << received_term << "]: " <<  cmd.value << " ";
        logs.push_back(log_entry<command>(received_term, cmd));
    }
    in.close();
    std::cout << std::endl << "load over!" << "log.size = " << logs.size()<< std::endl;


}

template<typename command>
void raft_storage<command>::save_to_local() {
    // Your code here
    std::unique_lock<std::mutex> lock(mtx);
//    std::cout << "in ~raft_storage" << std::endl;
//    std::cout << "file_path = " << file_path << std::endl;
    std::ofstream ofile(file_path.c_str());
    if (!ofile.is_open()) {
        std::cout << "output file do not open successfully!" << std::endl;
        return;
    }
    ofile << (int)role << std::endl << current_term << std::endl << voteFor << std::endl << last_received_RPC_time << std::endl << commitIndex << std::endl << lastApplied << std::endl;
    ofile << nextIndex.size() << std::endl << matchIndex.size() << std::endl;
    for (int i = 0; i < (int)nextIndex.size(); i++) {
        ofile << nextIndex[i] << std::endl;
    }
    ofile << std::endl;
    for (int i = 0; i < (int)matchIndex.size(); i++) {
        ofile << matchIndex[i] << std::endl;
    }
    ofile << logs.size() << std::endl;
    for (int i = 0; i < (int)logs.size(); i++) {
        log_entry<command> current_log_entry = logs[i];
        int com_to_string_size = current_log_entry.com.size();
        char *com_buf = (char*)malloc(sizeof(char) * com_to_string_size);
        current_log_entry.com.serialize(com_buf, com_to_string_size);
//        std::cout << "try write command value = " << current_log_entry.com.value << "buf=" << com_buf << std::endl;
        ofile << com_to_string_size << " " << current_log_entry.received_term << std::endl;
        for (int j = 0; j < com_to_string_size; j++) {
            ofile << (int)(com_buf[j]) << " ";
        }
        ofile << std::endl;
        free(com_buf);
    }
    ofile.close();
//    std::cout << "write over" << std::endl;
}

template<typename command>
raft_storage<command>::~raft_storage() {
    save_to_local();
}

#endif // raft_storage_h