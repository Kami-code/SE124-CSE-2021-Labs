// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "gettime.h"
using namespace std;

string remove_redundant(string s) {
    int s_length = s.length();
    int redundant_number = 0;
    for (int i = s_length - 1; i >= 0; i--) {
        if (s[i] == '/') {
            redundant_number ++;
        }
        else {
            break;
        }
    }
    return s.substr(0, s_length - redundant_number);
}

string add_redundant(string s, int upper_bound) {
    int s_length = s.length();
    assert(s_length <= upper_bound);
    for (int i = 0; i < (upper_bound - s_length); i++) {
        s += '/';
    }
    return s;
}

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum) {
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

bool chfs_client::isdir(inum inum) {
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    }
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool chfs_client::issymboliclink(inum inum) {
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymboliclink: %lld is a symbolic link\n", inum);
        return true;
    }
    printf("issymboliclink: %lld is not a symbolic link\n", inum);
    return false;
}

/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

unsigned int getcurrenttime() {
    struct timespec *tp = (struct timespec*) malloc(sizeof(struct  timespec*));
    clock_gettime(CLOCK_REALTIME, tp);
    unsigned int current_time = tp->tv_nsec;
    free(tp);
    return current_time;
}



int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)


// Only support set size of attr
int chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    printf("in chfs_setattr(size)\n");

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    extent_protocol::attr a;
    if (ec->getattr(ino, a) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    size_t current_size = a.size;
    if (current_size == size) {
        return r;
    }
    string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    if (current_size < size) {
        for (int i = 0; i < size - current_size; i++) {
            buf += '\0';
        }
    }
    else if (current_size > size) {
        buf = buf.substr(0, size);
    }
    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    return r;
}

int chfs_client::settimeandtype(inum ino, short type, unsigned int atime, unsigned int mtime, unsigned int ctime) {
    int r = OK;
    if (ec->setattr(ino, type, atime, mtime, ctime) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;

    if (lookup(parent, name, found, ino_out) != OK) {
        r = IOERR;
        return r;
    }
    if (found) { // we found the inode_number
        return r;
    }
    else {
        ec->create(extent_protocol::T_FILE, ino_out);
        assert(ino_out != 0);

        string string_name(name);
        stringstream stream;
        string inode_string;
        stream << ino_out;
        stream >> inode_string;

        string append_string = add_redundant(string_name, 64) + add_redundant(inode_string, 8);
        assert(append_string.length() == 72);

        string buf;
        if (ec->get(parent, buf) != extent_protocol::OK) {
            r = IOERR;
            return r;
        }
        buf += append_string;
        if (ec->put(parent, buf) != extent_protocol::OK) {
            r = IOERR;
            return r;
        }
        unsigned int current_time = getcurrenttime();
        settimeandtype(ino_out, extent_protocol::T_FILE, current_time, current_time, current_time);
        settimeandtype(parent, 0, 0, current_time, current_time);
    }
    return r;
}

//https://www.cnblogs.com/zhaojiedi1992/p/zhaojiedi_linux_031_linuxtime.html
int chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    printf("start mkdir \n");
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::list<dirent> list;
    readdir(parent, list);
    for(auto it = list.begin(); it!=list.end();it++){
        dirent current_dir = *it;
        if (strcmp(current_dir.name.c_str(), name) == 0) { //equal
            ino_out = current_dir.inum;
            r = EXIST;
            return r;
        }
    }
    printf("dir not existed, try create new! \n");
    // todo:handle inode type
    r = create(parent, name, mode, ino_out);


    unsigned int current_time = getcurrenttime();
    settimeandtype(ino_out, extent_protocol::T_DIR, current_time, current_time, current_time);
    settimeandtype(parent, 0, 0, current_time, current_time);
    printf("dir allocated inode_number = %d", ino_out);
    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    printf("you are in chfs_lookup parent_ino = %d, name = %s \n", parent, name);
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;
    if (readdir(parent, list) != OK) {
        r = IOERR;
        return r;
    }
    readdir(parent, list);
    for(auto it = list.begin(); it!=list.end();it++){
        dirent current_dir = *it;
        //printf("enum file name = %s, name = %s \n", current_dir.name.c_str(), name);

        if (strcmp(current_dir.name.c_str(), name) == 0) { //equal
            ino_out = current_dir.inum;
            //printf("euqal! found ino number = %d \n", ino_out);
            found = true;
            return r;
        }
    }
    found = false;
    printf("out chfs_lookup, sorry not found, name = %s\n", name);
    return r;
}




int chfs_client::readdir(inum dir, std::list<dirent> &list) {
    int r = OK;
    printf("\t\t enter readdir \n");
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    string buf;
    if (ec->get(dir, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    unsigned int current_time = getcurrenttime();
    settimeandtype(dir, 0, 0, current_time, current_time);

    int last_final_cursor = 0, current_cursor = 0;
    string dirname, inode_string;
    inum inode_number = 0;

    assert(buf.length() % 72 == 0);
    int files = buf.length() / 72;


    for (int file = 0; file < files; file++) {
        string file_name = buf.substr(file * 72, 64);
        string file_length = buf.substr(file * 72 + 64, 8);
        //std::cout << "file_name = " << file_name << " file_length = " << file_length << std::endl;
        file_name = remove_redundant(file_name);
        file_length = remove_redundant(file_length);
        int file_length_number = atoi(file_length.c_str());
        dirent current_dirent;
        current_dirent.name = file_name;
        current_dirent.inum = file_length_number;
        //std::cout << "ino string = " <<  buf.substr(file * 72 + 64, 8) << std::endl << file_length << std::endl << " ino length number = " <<file_length_number << std::endl;
        list.push_back(current_dirent);
    }

    printf("\t\t out readdir \n");
    return r;

}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    if (isfile(ino) || isdir(ino)) {
        printf("start reading file or dir! \n");
        string buf;
        if (ec->get(ino, buf) != extent_protocol::OK) {
            r = IOERR;
            return r;
        }
        printf("buf.length() = %d, want_to_read_bytes = %d, offset = %d\n", buf.length(), size, off);
        unsigned int current_time = getcurrenttime();
        settimeandtype(ino, 0, current_time, 0, 0);

        if (off >= buf.length()) {
            data = "";
        }
        else if ((off + size) < buf.length()) {
            data = buf.substr(off, size);
        }
        else {
            data = buf.substr(off);
        }

        printf("data.length() = %d\n", data.length());
        return r;
    }
    else if (issymboliclink(ino)) {
        printf("start reading symbolic link! \n");
        if (ec->get(ino, data) != extent_protocol::OK) {
            r = IOERR;
            return r;
        }
        std::cout << "the link content is " << data << std::endl;
        unsigned int current_time = getcurrenttime();
        settimeandtype(ino, 0, current_time, 0, 0);
    }
    return r;
}

int chfs_client::write(inum ino, size_t size, off_t off, const char *data, size_t &bytes_written) {
    int r = OK;
    printf("start writing! \n");
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    string buf;
    if (ec->get(ino, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    printf("current_size = %d, need_to_write_byte = %d, off = %d\n", buf.length(), size, off);
    bytes_written = 0;
    while(off > buf.length()) buf += '\0'; //fill the gap

    string prefix_buf = buf.substr(0, off);
    printf("prefix_buf_length = %d\n",prefix_buf.length());
    printf("strlen(data) = %d\n", strlen(data));
    for (int i = 0; i < size; ++i) {
        prefix_buf += data[i];
        bytes_written++;
    }
    assert(prefix_buf.length() == (off + size));
    if ((off + size) < buf.length()) {
        prefix_buf += buf.substr(size + off);
    }

    if (ec->put(ino, prefix_buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    unsigned int current_time = getcurrenttime();
    settimeandtype(ino, 0, 0, current_time, current_time);
    printf("bytes_written = %d\n", bytes_written);
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found;
    inum ino_out;
    if (lookup(parent, name, found, ino_out) != OK) {
        r = IOERR;
        return r;
    }
    if (!found) return r;

    if (ec->remove(ino_out) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    string buf;
    if (ec->get(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }

    string dirname, inode_string;
    inum inode_number = 0;

    assert(buf.length() % 72 == 0);
    int files = buf.length() / 72;

    int flag = -1;
    for (int file = 0; file < files; file++) {
        string file_name = buf.substr(file * 72, 64);
        string file_length = buf.substr(file * 72 + 64, 8);
        file_name = remove_redundant(file_name);
        if (strcmp(file_name.c_str(), name) == 0) {
            flag = file;
            break;
        }
    }
    assert(flag != -1);
    string result_buf = buf.substr(0, flag * 72);
    if (flag != files) {
        result_buf += buf.substr((flag + 1) * 72);
    }
    if (ec->put(parent, result_buf) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    unsigned int current_time = getcurrenttime();
    settimeandtype(parent, 0, 0, current_time, current_time);
    return r;
}

int chfs_client::symlink(inum parent, const char *link, const char *path, inum& ino_out)
{
    int r = OK;
    printf("start symlink path = %s, link = %s\n", path, link);
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    std::list<dirent> list;
    printf("try to enter readdir \n");
    readdir(parent, list);
    printf("out readdir \n");
    for(auto it = list.begin(); it!=list.end();it++){
        dirent current_dir = *it;
        //printf("compare current_dir = %s, link = %s", current_dir.name.c_str(), link);
        if (strcmp(current_dir.name.c_str(), path) == 0) { //equal
            ino_out = current_dir.inum;
            r = EXIST;
            return r;
        }
    }
    printf("link not existed, try create new! filename = %s\n", path);

    r = create(parent, path, 1, ino_out);
    if (r != OK) {
        return r;
    }

    unsigned int current_time = getcurrenttime();
    settimeandtype(ino_out, extent_protocol::T_SYMLINK, current_time, current_time, current_time);
    size_t byte_written;
    r = write(ino_out, strlen(link), 0, link, byte_written);
    settimeandtype(parent, 0, 0, current_time, current_time);

    printf("symlink return !!\n");
    return r;
}

int chfs_client::readlink(chfs_client::inum ino, std::string &result) {
    int r;
    if (ec->get(ino, result) != extent_protocol::OK) {
        r = IOERR;
        return r;
    }
    printf("readlink try to get %s \n", result.c_str());
    return r;
}