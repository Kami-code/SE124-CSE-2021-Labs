#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return 19 + key.size() + value.size();
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    buf[0] = (int)cmd_tp;
    std::string key_size_string = std::to_string(key.size());
    std::string value_size_string = std::to_string(value.size());
    while (key_size_string.size() < 9) {
        key_size_string = "0" + key_size_string;
    }
    while (value_size_string.size() < 9) {
        value_size_string = "0" + value_size_string;
    }
    memcpy(buf + 1, key_size_string.c_str(), 9);
    memcpy(buf + 10, value_size_string.c_str(), 9);
    //start from buf + 19
    memcpy(buf + 19, key.c_str(), key.size());
    memcpy(buf + 19 + key.size(), value.c_str(), value.size());
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    cmd_tp = (command_type)buf[0];
    char *key_size_buf = (char*)malloc(sizeof(char) * 9), *value_size_buf = (char*)malloc(sizeof(char) * 9);
    memcpy(key_size_buf, buf + 1, 9);
    key_size_buf[9] = 0;
    memcpy(value_size_buf, buf + 10, 9);
    value_size_buf[9] = 0;
    std::cout << "key_size_buf = " << key_size_buf << std::endl;
    int key_size = atoi(key_size_buf), value_size = atoi(value_size_buf);
    std::cout << "keysize = " << key_size << " value_size = " << value_size << std::endl;
    char *key_buf = (char *)malloc(sizeof(char) * key_size);
    memcpy(key_buf, buf + 19, key_size);
    key_buf[key_size] = 0;
    char *value_buf = (char *)malloc(sizeof(char) * value_size);
    memcpy(value_buf, buf + 19 + key_size, value_size);
    value_buf[value_size] = 0;
    key = std::string(key_buf); value = std::string(value_buf);
    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m << cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int a;
    u >> a;
    cmd.cmd_tp = (kv_command::command_type)a;
    u >> cmd.key;
    u >> cmd.value;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    std::cout << "hello!" << std::endl;
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->key = kv_cmd.key;
    std::map<std::string, std::string>::iterator it;
    it = hashMap.find(kv_cmd.key);
    if (kv_cmd.cmd_tp == kv_command::CMD_NONE) {

    }
    else if (kv_cmd.cmd_tp == kv_command::CMD_GET) {
        // Get       T      key     value
        // Get       F      key      ""
        if (it != hashMap.end()) {
            std::cout << "CMD GET and has old value!" << std::endl;
            kv_cmd.res->value = it->second;
            kv_cmd.res->succ = true;
        }
        else {
            std::cout << "CMD GET and has new value!" << std::endl;
            kv_cmd.res->value = "";
            kv_cmd.res->succ = false;
        }
    }
    else if (kv_cmd.cmd_tp == kv_command::CMD_PUT) {
        // Put       T      key   new_value
        // Put  F(replace)  key   old_value
        if (it != hashMap.end()) {
            std::cout << "CMD PUT and has old value!" << std::endl;
            kv_cmd.res->value = it->second;
            kv_cmd.res->succ = false;
        }
        else {
            std::cout << "CMD PUT and has new value!" << std::endl;
            hashMap[kv_cmd.key] = kv_cmd.value;
            kv_cmd.res->value = kv_cmd.value;
            kv_cmd.res->succ = true;
        }
    }
    else if (kv_cmd.cmd_tp == kv_command::CMD_DEL) {
        // Del       T      key   old_value
        // Del       F      key      ""
        if (it != hashMap.end()) {
            kv_cmd.res->value = it->second;
            hashMap.erase(it);
            kv_cmd.res->succ = true;
        }
        else {
            kv_cmd.res->succ = false;
        }
    }

    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    return;    
}
